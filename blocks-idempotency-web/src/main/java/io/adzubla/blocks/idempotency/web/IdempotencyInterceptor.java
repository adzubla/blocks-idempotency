package io.adzubla.blocks.idempotency.web;

import io.adzubla.blocks.idempotency.annotation.Idempotent;
import io.adzubla.blocks.idempotency.config.IdempotencyProperties;
import io.adzubla.blocks.idempotency.engine.EngineDecision;
import io.adzubla.blocks.idempotency.engine.IdempotencyEngine;
import io.adzubla.blocks.idempotency.engine.IdempotencyEngineRegistry;
import io.adzubla.blocks.idempotency.fingerprint.Fingerprint;
import io.adzubla.blocks.idempotency.key.BodyFieldKeyStrategy;
import io.adzubla.blocks.idempotency.key.KeyFormat;
import io.adzubla.blocks.idempotency.key.PrincipalClaimResolver;
import io.adzubla.blocks.idempotency.model.CachedResponse;
import io.adzubla.blocks.idempotency.model.EffectiveKey;
import io.adzubla.blocks.idempotency.policy.IdempotencyPolicy;
import io.adzubla.blocks.idempotency.policy.PolicyResolver;
import io.adzubla.blocks.idempotency.response.ResponseCapture;
import io.adzubla.blocks.idempotency.response.ResponseReplayer;
import io.adzubla.blocks.idempotency.store.EffectRolledBackException;
import io.adzubla.blocks.idempotency.web.key.HeaderKeyStrategy;
import io.adzubla.blocks.idempotency.web.key.HttpEffectiveKeyFactory;
import jakarta.servlet.DispatcherType;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatusCode;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.util.ContentCachingResponseWrapper;
import org.springframework.web.util.WebUtils;

import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * Reads the resolved handler's {@link Idempotent} annotation and drives the
 * {@link IdempotencyEngine}: replays a completed record in {@link #preHandle},
 * or lets the handler execute and, in {@link #afterCompletion} (fired
 * regardless of whether the handler threw), either completes the record with
 * its 2xx response or releases the reservation on a non-2xx/exception. For a
 * handler that completes asynchronously ({@code CompletableFuture}, {@code
 * DeferredResult}, {@code Callable}), {@code afterCompletion} fires after the
 * async dispatch that writes the result, possibly on a different thread than
 * {@code preHandle} - fine for a store with no thread affinity, but not for
 * {@code postgres}, whose reservation is a thread-bound transaction (startup
 * validation rejects that combination).
 *
 * <p>Key resolution: header or body-field key strategy (whichever
 * annotation attribute is non-empty - exactly one is guaranteed by startup
 * validation). {@link PolicyResolver} settles {@code store}/{@code
 * ttl}/{@code onStoreFailure}/{@code whenInProgress}/{@code keyRequired} per
 * request; {@code store} routes to the matching engine via {@link
 * IdempotencyEngineRegistry}, and every other attribute is consumed by
 * behavior. The lock TTL, wait timeout, and max body size have no
 * per-endpoint override, so they're read directly off the global properties.
 *
 * <p>Every rejecting outcome is thrown as an {@link IdempotencyException}
 * rather than written to the response directly: {@code preHandle} runs inside
 * {@code DispatcherServlet}'s own try/catch, so the exception reaches Spring's
 * {@code HandlerExceptionResolver} chain just like one thrown from a handler
 * method - letting {@link IdempotencyExceptionHandler} (or an application's
 * own {@code @ControllerAdvice}) translate it into the response.
 */
public class IdempotencyInterceptor implements HandlerInterceptor {

    private static final Logger log = LoggerFactory.getLogger(IdempotencyInterceptor.class);

    private static final String ATTR_EFFECTIVE_KEY = IdempotencyInterceptor.class.getName() + ".effectiveKey";
    private static final String ATTR_POLICY = IdempotencyInterceptor.class.getName() + ".policy";
    private static final String ATTR_FENCE_TOKEN = IdempotencyInterceptor.class.getName() + ".fenceToken";
    private static final String ATTR_ENGINE = IdempotencyInterceptor.class.getName() + ".engine";

    private static final byte[] NO_BODY = new byte[0];

    private final IdempotencyEngineRegistry engineRegistry;
    private final IdempotencyProperties properties;
    private final PrincipalClaimResolver principalClaimResolver;

    public IdempotencyInterceptor(IdempotencyEngineRegistry engineRegistry, IdempotencyProperties properties,
            PrincipalClaimResolver principalClaimResolver) {
        this.engineRegistry = engineRegistry;
        this.properties = properties;
        this.principalClaimResolver = principalClaimResolver;
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        Idempotent annotation = idempotentAnnotation(handler);
        if (annotation == null) {
            return true;
        }
        if (request.getDispatcherType() == DispatcherType.ASYNC) {
            // The handler completed asynchronously and Spring MVC is
            // re-dispatching to write its result. The key was already resolved
            // and reserved on the first dispatch; afterCompletion, which only
            // fires once this dispatch finishes, settles it.
            return true;
        }

        IdempotencyPolicy policy = PolicyResolver.resolve(annotation, properties);

        // Exactly one of header()/fieldPath() is set - validated at startup. A header
        // key is resolved (and below, validated) before the body is touched, so a request that's
        // rejected or passed through for its key alone never has its body buffered.
        byte[] body = annotation.header().isEmpty() ? requestBody(request) : null;
        Optional<String> rawKey = annotation.header().isEmpty()
                ? BodyFieldKeyStrategy.resolve(body, annotation.fieldPath())
                : HeaderKeyStrategy.resolve(request, annotation.header());
        if (rawKey.isEmpty()) {
            if (policy.keyRequired()) {
                log.debug("Idempotency key missing but required: {} {}", request.getMethod(), request.getRequestURI());
                throw new IdempotencyKeyRequiredException();
            }
            // keyRequired=false: protection is a client opt-in - pass through
            // unprotected, nothing cached.
            return true;
        }
        if (!KeyFormat.isValid(rawKey.get(), properties.getKey().getMaxLength())) {
            log.debug("Idempotency key value invalid (size/charset): {} {}", request.getMethod(), request.getRequestURI());
            throw new IdempotencyKeyInvalidException();
        }

        if (body == null) {
            body = requestBody(request);
        }

        EffectiveKey key = HttpEffectiveKeyFactory.create(request, rawKey.get(), properties.getScope().isPrincipalEnabled(),
                properties.getScope().getPrincipalClaim(), principalClaimResolver);
        // The fingerprint's target is the route plus the canonical query; the
        // effective key's route stays the bare path.
        String fingerprintTarget = key.route() + canonicalQuery(request);
        String fingerprint = Fingerprint.sha256(fingerprintTarget, key.handler(), body);

        IdempotencyEngine engine = engineRegistry.engine(policy.store());
        EngineDecision decision = engine.before(key, fingerprint, properties.getLockTtl(), policy.onStoreFailure(),
                policy.whenInProgress(), properties.getWaitTimeout());

        if (decision instanceof EngineDecision.Replay replay) {
            ResponseReplayer.replay(response, replay.response(), properties.getReplay().getHeaderName());
            return false;
        }
        if (decision instanceof EngineDecision.Collision) {
            throw new IdempotencyCollisionException();
        }
        if (decision instanceof EngineDecision.Reject reject) {
            throw new IdempotencyConflictException(reject.reason(), reject.retryAfter());
        }
        // Terminal: the effect ran but its response isn't replayable - no Retry-After, retrying can't help.
        if (decision instanceof EngineDecision.Unavailable) {
            throw new IdempotencyResponseUnavailableException();
        }
        if (decision instanceof EngineDecision.AlreadyProcessed) {
            // Only a caller that completes with CachedResponse.empty() (the messaging advice) gets this;
            // HTTP always completes with the real response, so reaching here is a bug, not a duplicate.
            throw new IllegalStateException("Unexpected AlreadyProcessed decision for " + request.getMethod() + " "
                    + request.getRequestURI() + ": an HTTP record was completed without a response");
        }
        if (decision instanceof EngineDecision.FailClosed) {
            throw new IdempotencyFailClosedException();
        }
        if (decision instanceof EngineDecision.ProceedUnprotected) {
            // The store is down and the posture is fail-open: run the
            // handler, but there's no reservation to track - skip the
            // attributes afterCompletion keys off, and let it no-op.
            return true;
        }
        if (decision instanceof EngineDecision.Proceed proceed) {
            CapturingHttpServletResponse capturing = WebUtils.getNativeResponse(response, CapturingHttpServletResponse.class);
            if (capturing != null) {
                capturing.startCapture();
            }
            request.setAttribute(ATTR_EFFECTIVE_KEY, key);
            request.setAttribute(ATTR_POLICY, policy);
            request.setAttribute(ATTR_FENCE_TOKEN, proceed.fenceToken());
            request.setAttribute(ATTR_ENGINE, engine);
            return true;
        }
        return true;
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) {
        EffectiveKey key = (EffectiveKey) request.getAttribute(ATTR_EFFECTIVE_KEY);
        if (key == null) {
            return;
        }

        String fenceToken = (String) request.getAttribute(ATTR_FENCE_TOKEN);
        IdempotencyEngine engine = (IdempotencyEngine) request.getAttribute(ATTR_ENGINE);
        if (ex != null || !HttpStatusCode.valueOf(response.getStatus()).is2xxSuccessful()) {
            // Only 2xx is cached; a non-2xx or a thrown exception releases the
            // reservation so a genuine retry re-executes rather than replaying
            // a stale error.
            engine.release(key, fenceToken);
            return;
        }

        IdempotencyPolicy policy = (IdempotencyPolicy) request.getAttribute(ATTR_POLICY);
        try {
            engine.complete(key, fenceToken, capture(response), policy.ttl());
        } catch (EffectRolledBackException e) {
            // The store rolled the handler's effect back at commit. The 2xx the
            // handler produced is still only buffered (IdempotencyFilter), so
            // replace it rather than tell the client something happened that didn't.
            log.error("Idempotent effect rolled back at commit - responding 500 instead of the handler's {}: {} {} key={}",
                    response.getStatus(), key.route(), key.handler(), key.value(), e);
            if (!response.isCommitted()) {
                response.reset();
                response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            }
        }
    }

    /**
     * The captured 2xx response. It is buffered by the {@link ContentCachingResponseWrapper}
     * that {@link CapturingHttpServletResponse#startCapture()} put in place, which may sit underneath wrappers
     * added by later filters (e.g. Spring Security's). If that wrapper isn't in
     * the chain at all there is nothing to capture, but the effect did run - the
     * record is completed without a body ({@code response_unavailable} on a
     * repeat) rather than left to expire and re-execute.
     */
    private CachedResponse capture(HttpServletResponse response) {
        ContentCachingResponseWrapper wrapper = WebUtils.getNativeResponse(response, ContentCachingResponseWrapper.class);
        if (wrapper == null) {
            log.warn("IdempotencyFilter is not in the filter chain - the response can't be captured for replay");
            return new CachedResponse(response.getStatus(), Map.of(), null);
        }
        return ResponseCapture.capture(wrapper, properties.getReplay().getHeaderDenylist(), properties.getMaxBodySize());
    }

    /**
     * The bytes the body-field key and the fingerprint are taken from.
     *
     * <p>A JSON (or any other raw) body is buffered through the {@link
     * CachedBodyHttpServletRequest} {@link IdempotencyFilter} put in place,
     * which may sit underneath wrappers added by later filters. A form post is
     * not read from the stream - the container parses it into parameters, and
     * draining the stream here would leave the handler without them - so its
     * fingerprint input is the parameters themselves, in a canonical order. A
     * multipart body is left to the container for the same reason and
     * contributes nothing: such a request is keyed and deduplicated, but a
     * different upload under the same key is not detected as a collision.
     */
    private byte[] requestBody(HttpServletRequest request) {
        if (isForm(request)) {
            return canonicalForm(request.getParameterMap());
        }
        if (contentTypeOf(request).startsWith("multipart/")) {
            return NO_BODY;
        }
        CachedBodyHttpServletRequest cached = WebUtils.getNativeRequest(request, CachedBodyHttpServletRequest.class);
        return cached != null ? cached.getBody(properties.getMaxRequestBodySize().toBytes()) : NO_BODY;
    }

    /**
     * The query string's contribution to the fingerprint: {@code "?"} plus its parameters in
     * canonical order, or empty when there is none. The query is not part of the route (that is
     * {@code getRequestURI()}), so without this two requests reusing a key with different
     * query parameters would replay instead of colliding. Parameters are sorted by name (values
     * keep their order), the same canonicalization as a form body, so reordering alone doesn't
     * collide. A form post's parameters already include the query string, so it contributes
     * nothing extra there. An absent query leaves the fingerprint exactly as before.
     */
    private static String canonicalQuery(HttpServletRequest request) {
        String query = request.getQueryString();
        if (query == null || query.isEmpty() || isForm(request)) {
            return "";
        }
        Map<String, List<String>> parameters = new LinkedHashMap<>();
        for (String pair : query.split("&")) {
            if (pair.isEmpty()) {
                continue;
            }
            int eq = pair.indexOf('=');
            String name = decodeOrRaw(eq < 0 ? pair : pair.substring(0, eq));
            String value = eq < 0 ? "" : decodeOrRaw(pair.substring(eq + 1));
            parameters.computeIfAbsent(name, n -> new ArrayList<>()).add(value);
        }
        Map<String, String[]> asArrays = new LinkedHashMap<>();
        parameters.forEach((name, values) -> asArrays.put(name, values.toArray(String[]::new)));
        return "?" + new String(canonicalForm(asArrays), StandardCharsets.UTF_8);
    }

    /**
     * Decodes one query component, or keeps it as the raw text when it isn't valid
     * percent-encoding ({@code %zz}). The fingerprint only needs to be deterministic, not to
     * reject the request. A parameter without {@code =} and one with an empty value fingerprint
     * the same, as do {@code +} and {@code %20}, mirroring how the container parses them.
     */
    private static String decodeOrRaw(String component) {
        try {
            return URLDecoder.decode(component, StandardCharsets.UTF_8);
        } catch (IllegalArgumentException malformed) {
            return component;
        }
    }

    private static boolean isForm(HttpServletRequest request) {
        return contentTypeOf(request).startsWith("application/x-www-form-urlencoded");
    }

    private static String contentTypeOf(HttpServletRequest request) {
        String contentType = request.getContentType();
        return contentType == null ? "" : contentType.toLowerCase(Locale.ROOT);
    }

    private static byte[] canonicalForm(Map<String, String[]> parameters) {
        StringBuilder form = new StringBuilder();
        new TreeMap<>(parameters).forEach((name, values) -> {
            for (String value : values) {
                form.append(URLEncoder.encode(name, StandardCharsets.UTF_8)).append('=')
                        .append(URLEncoder.encode(value, StandardCharsets.UTF_8)).append('&');
            }
        });
        return form.toString().getBytes(StandardCharsets.UTF_8);
    }

    private Idempotent idempotentAnnotation(Object handler) {
        if (!(handler instanceof HandlerMethod handlerMethod)) {
            return null;
        }
        return handlerMethod.getMethodAnnotation(Idempotent.class);
    }
}
