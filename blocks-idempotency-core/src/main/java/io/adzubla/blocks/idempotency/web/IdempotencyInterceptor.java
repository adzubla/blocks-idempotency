package io.adzubla.blocks.idempotency.web;

import io.adzubla.blocks.idempotency.annotation.Idempotent;
import io.adzubla.blocks.idempotency.config.IdempotencyProperties;
import io.adzubla.blocks.idempotency.engine.EngineDecision;
import io.adzubla.blocks.idempotency.engine.IdempotencyEngine;
import io.adzubla.blocks.idempotency.engine.IdempotencyEngineRegistry;
import io.adzubla.blocks.idempotency.fingerprint.Fingerprint;
import io.adzubla.blocks.idempotency.key.BodyFieldKeyStrategy;
import io.adzubla.blocks.idempotency.key.EffectiveKeyFactory;
import io.adzubla.blocks.idempotency.key.HeaderKeyStrategy;
import io.adzubla.blocks.idempotency.key.KeyFormat;
import io.adzubla.blocks.idempotency.key.PrincipalClaimResolver;
import io.adzubla.blocks.idempotency.model.CachedResponse;
import io.adzubla.blocks.idempotency.model.EffectiveKey;
import io.adzubla.blocks.idempotency.policy.IdempotencyPolicy;
import io.adzubla.blocks.idempotency.policy.PolicyResolver;
import io.adzubla.blocks.idempotency.response.ResponseCapture;
import io.adzubla.blocks.idempotency.response.ResponseReplayer;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatusCode;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.util.ContentCachingResponseWrapper;

import java.util.Optional;

/**
 * Reads the resolved handler's {@link Idempotent} annotation and drives the
 * {@link IdempotencyEngine}: replays a completed record in {@link #preHandle},
 * or lets the handler execute and, in {@link #afterCompletion} (fired
 * regardless of whether the handler threw), either completes the record with
 * its 2xx response or releases the reservation on a non-2xx/exception.
 *
 * <p>Slice 001-012 scope: header or body-field key strategy (whichever
 * annotation attribute is non-empty - exactly one is guaranteed by startup
 * validation, Slice 010). {@link PolicyResolver} settles {@code store}/{@code
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

        IdempotencyPolicy policy = PolicyResolver.resolve(annotation, properties);

        byte[] body = request instanceof CachedBodyHttpServletRequest cached ? cached.getBody() : new byte[0];
        // Exactly one of header()/fieldPath() is set - validated at startup (Slice 010).
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

        EffectiveKey key = EffectiveKeyFactory.create(request, rawKey.get(), properties.getScope().isPrincipalEnabled(),
                properties.getScope().getPrincipalClaim(), principalClaimResolver);
        String fingerprint = Fingerprint.sha256(key.route(), key.handler(), body);

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
        if (key == null || !(response instanceof ContentCachingResponseWrapper wrapper)) {
            return;
        }

        String fenceToken = (String) request.getAttribute(ATTR_FENCE_TOKEN);
        IdempotencyEngine engine = (IdempotencyEngine) request.getAttribute(ATTR_ENGINE);
        if (ex != null || !HttpStatusCode.valueOf(wrapper.getStatus()).is2xxSuccessful()) {
            // Only 2xx is cached; a non-2xx or a thrown exception releases the
            // reservation so a genuine retry re-executes rather than replaying
            // a stale error.
            engine.release(key, fenceToken);
            return;
        }

        IdempotencyPolicy policy = (IdempotencyPolicy) request.getAttribute(ATTR_POLICY);
        CachedResponse captured = ResponseCapture.capture(wrapper, properties.getReplay().getHeaderDenylist(), properties.getMaxBodySize());
        engine.complete(key, fenceToken, captured, policy.ttl());
    }

    private Idempotent idempotentAnnotation(Object handler) {
        if (!(handler instanceof HandlerMethod handlerMethod)) {
            return null;
        }
        return handlerMethod.getMethodAnnotation(Idempotent.class);
    }
}
