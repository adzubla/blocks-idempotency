package io.adzubla.blocks.idempotency.store.redis;

import io.adzubla.blocks.idempotency.model.CachedResponse;
import io.adzubla.blocks.idempotency.model.EffectiveKey;
import io.adzubla.blocks.idempotency.model.IdempotencyRecord;
import io.adzubla.blocks.idempotency.model.RecordState;
import io.adzubla.blocks.idempotency.model.ReservationResult;
import io.adzubla.blocks.idempotency.store.IdempotencyStore;
import io.adzubla.blocks.idempotency.store.StoreUnavailableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import tools.jackson.core.JacksonException;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * Best-effort {@link IdempotencyStore} backed by Redis.
 *
 * <p>Design (ADR 0001, Redis key structure): one Redis Hash per record, keyed by
 * {@code {prefix}{sha256(method \0 path \0 principal \0 keyValue)}} - hashed for
 * collision-safety and bounded memory, single key per record so it's Cluster-safe
 * (no cross-slot operations). Reserve and complete are single Lua scripts: Redis
 * runs each script to completion without interleaving any other command, so the
 * "does this key already exist" check and the write that follows it are atomic
 * with no separate locking needed. Lifecycle rides the key's own TTL - reserve →
 * {@code lock-ttl} (a bare {@code PEXPIRE}, not a conditional supersession dance
 * like Postgres's: an expired key is simply gone from Redis, {@code EXISTS}
 * naturally reports it absent); complete → {@code response-ttl}; release → a
 * fenced {@code DEL}. Both {@code complete}/{@code release} no-op (via the Lua
 * script's own fence-token check) when the caller's token no longer matches the
 * hash's stored one - the reservation was superseded by a fresh caller after this
 * one's lock-ttl passed.
 *
 * <p>Hash fields: {@code fingerprint}, {@code token} (fence token, written at
 * reserve and left alone by {@code complete}); {@code status}/{@code headers}
 * (JSON) are only present once completed - their absence, not a separate state
 * field, is what {@link #mapRecord} reads as {@link RecordState#IN_PROGRESS}.
 * {@code body} is Base64-encoded (a Hash field is a Redis string; the response
 * body is arbitrary bytes, not necessarily UTF-8) and its absence on an otherwise
 * completed record means {@code response_unavailable} (oversized body - see
 * {@code ResponseCapture}), mirroring {@code response_body IS NULL} in the
 * Postgres store.
 */
public class RedisIdempotencyStore implements IdempotencyStore {

    private static final Logger log = LoggerFactory.getLogger(RedisIdempotencyStore.class);

    /** Bean qualifier used by {@code @Idempotent(store = ...)}. */
    public static final String QUALIFIER = "redis";

    // KEYS[1] = record key. ARGV[1] = fingerprint, ARGV[2] = fence token,
    // ARGV[3] = lock-ttl millis. Returns 1 (won) or 0 (a live record already
    // exists - an expired one is simply absent from Redis, no separate
    // supersession check needed, unlike Postgres's conditional DO UPDATE).
    private static final RedisScript<Long> RESERVE_SCRIPT = RedisScript.of("""
            if redis.call('EXISTS', KEYS[1]) == 1 then
                return 0
            end
            redis.call('HSET', KEYS[1], 'fingerprint', ARGV[1], 'token', ARGV[2])
            redis.call('PEXPIRE', KEYS[1], ARGV[3])
            return 1
            """, Long.class);

    // KEYS[1] = record key. ARGV[1] = expected fence token, ARGV[2] = status,
    // ARGV[3] = headers JSON, ARGV[4] = Base64 body, ARGV[5] = "1"/"0" whether
    // the body is present (a Redis Hash field can't itself be null - this
    // flag decides HSET vs HDEL for 'body', see RedisIdempotencyStore's class
    // javadoc), ARGV[6] = response-ttl millis. Returns 1 (completed) or 0
    // (fenced off - the token no longer matches, a no-op per the SPI contract).
    private static final RedisScript<Long> COMPLETE_SCRIPT = RedisScript.of("""
            local currentToken = redis.call('HGET', KEYS[1], 'token')
            if not currentToken or currentToken ~= ARGV[1] then
                return 0
            end
            redis.call('HSET', KEYS[1], 'status', ARGV[2], 'headers', ARGV[3])
            if ARGV[5] == '1' then
                redis.call('HSET', KEYS[1], 'body', ARGV[4])
            else
                redis.call('HDEL', KEYS[1], 'body')
            end
            redis.call('PEXPIRE', KEYS[1], ARGV[6])
            return 1
            """, Long.class);

    // KEYS[1] = record key. ARGV[1] = expected fence token. Returns 1
    // (released) or 0 (fenced off - a no-op, same reasoning as COMPLETE_SCRIPT).
    private static final RedisScript<Long> RELEASE_SCRIPT = RedisScript.of("""
            local currentToken = redis.call('HGET', KEYS[1], 'token')
            if not currentToken or currentToken ~= ARGV[1] then
                return 0
            end
            redis.call('DEL', KEYS[1])
            return 1
            """, Long.class);

    private final StringRedisTemplate redis;
    private final ObjectMapper headerMapper;
    private final String keyPrefix;

    public RedisIdempotencyStore(StringRedisTemplate redis, ObjectMapper headerMapper, String keyPrefix) {
        this.redis = redis;
        this.headerMapper = headerMapper;
        this.keyPrefix = keyPrefix;
    }

    @Override
    public ReservationResult reserve(EffectiveKey key, String fingerprint, Duration lockTtl) {
        String redisKey = redisKey(key);
        String fenceToken = UUID.randomUUID().toString();
        return guarded(() -> {
            Long won = redis.execute(RESERVE_SCRIPT, List.of(redisKey), fingerprint, fenceToken, String.valueOf(lockTtl.toMillis()));
            if (Long.valueOf(1L).equals(won)) {
                return ReservationResult.reserved(fenceToken);
            }
            IdempotencyRecord existing = findRecord(redisKey).orElseThrow(() ->
                    new IllegalStateException("reservation conflict but no record visible for " + key));
            return ReservationResult.exists(existing);
        });
    }

    @Override
    public Optional<IdempotencyRecord> find(EffectiveKey key) {
        return guarded(() -> findRecord(redisKey(key)));
    }

    @Override
    public void complete(EffectiveKey key, String fenceToken, CachedResponse response, Duration responseTtl) {
        boolean hasBody = response.hasBody();
        String bodyEncoded = hasBody ? Base64.getEncoder().encodeToString(response.body()) : "";
        guarded(() -> redis.execute(COMPLETE_SCRIPT, List.of(redisKey(key)), fenceToken, String.valueOf(response.status()),
                toJson(response.headers()), bodyEncoded, hasBody ? "1" : "0", String.valueOf(responseTtl.toMillis())));
    }

    @Override
    public void release(EffectiveKey key, String fenceToken) {
        guarded(() -> redis.execute(RELEASE_SCRIPT, List.of(redisKey(key)), fenceToken));
    }

    /** Translates a genuine Redis connection/timeout failure into {@link StoreUnavailableException} (Slice 015). */
    private <T> T guarded(Supplier<T> redisOperation) {
        try {
            return redisOperation.get();
        } catch (DataAccessResourceFailureException | QueryTimeoutException e) {
            log.warn("Redis idempotency store unavailable", e);
            throw new StoreUnavailableException("Redis is unavailable", e);
        }
    }

    private Optional<IdempotencyRecord> findRecord(String redisKey) {
        Map<String, String> fields = redis.<String, String>opsForHash().entries(redisKey);
        return mapRecord(fields);
    }

    private Optional<IdempotencyRecord> mapRecord(Map<String, String> fields) {
        if (fields.isEmpty()) {
            return Optional.empty();
        }
        String fingerprint = fields.get("fingerprint");
        if (!fields.containsKey("status")) {
            return Optional.of(new IdempotencyRecord(RecordState.IN_PROGRESS, fingerprint, null));
        }
        int status = Integer.parseInt(fields.get("status"));
        Map<String, List<String>> headers = parseHeaders(fields.get("headers"));
        byte[] body = fields.containsKey("body") ? Base64.getDecoder().decode(fields.get("body")) : null;
        return Optional.of(new IdempotencyRecord(RecordState.COMPLETED, fingerprint, new CachedResponse(status, headers, body)));
    }

    private String redisKey(EffectiveKey key) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(key.method().getBytes(StandardCharsets.UTF_8));
            digest.update((byte) 0);
            digest.update(key.path().getBytes(StandardCharsets.UTF_8));
            digest.update((byte) 0);
            digest.update(key.principal().getBytes(StandardCharsets.UTF_8));
            digest.update((byte) 0);
            digest.update(key.value().getBytes(StandardCharsets.UTF_8));
            return keyPrefix + HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }

    private String toJson(Map<String, List<String>> headers) {
        try {
            return headerMapper.writeValueAsString(headers);
        } catch (JacksonException e) {
            throw new IllegalStateException("failed to serialize response headers", e);
        }
    }

    private Map<String, List<String>> parseHeaders(String json) {
        if (json == null) {
            return Map.of();
        }
        try {
            return headerMapper.readValue(json, new TypeReference<Map<String, List<String>>>() {
            });
        } catch (JacksonException e) {
            throw new IllegalStateException("failed to parse response headers", e);
        }
    }
}
