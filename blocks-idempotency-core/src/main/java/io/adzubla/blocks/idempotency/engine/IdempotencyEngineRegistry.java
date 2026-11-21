package io.adzubla.blocks.idempotency.engine;

import io.adzubla.blocks.idempotency.metrics.IdempotencyMetrics;
import io.adzubla.blocks.idempotency.store.IdempotencyStore;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * One {@link IdempotencyEngine} per store qualifier, so a request routes to the
 * engine backed by the store its endpoint resolved (ADR 0001: store is chosen
 * per {@code @Idempotent} endpoint, not once for the whole application). Built
 * once at startup from every {@link IdempotencyStore} bean in the context -
 * the web module's {@code IdempotentHandlerValidator}
 * already guarantees every endpoint's resolved qualifier has a matching store
 * bean, so {@link #engine} throwing signals that invariant was somehow
 * violated, not a normal outcome.
 */
public class IdempotencyEngineRegistry {

    private final Map<String, IdempotencyEngine> enginesByQualifier;

    /**
     * Builds one engine per store.
     *
     * @param storesByQualifier every store bean, keyed by its qualifier
     * @param pollInterval      base delay between WAIT-mode polls
     * @param pollJitter        extra random delay added to each poll
     * @param metrics           shared by every engine
     */
    public IdempotencyEngineRegistry(Map<String, IdempotencyStore> storesByQualifier, Duration pollInterval, Duration pollJitter,
            IdempotencyMetrics metrics) {
        Map<String, IdempotencyEngine> engines = new LinkedHashMap<>();
        storesByQualifier.forEach((qualifier, store) -> engines.put(qualifier, new IdempotencyEngine(store, pollInterval, pollJitter, metrics)));
        this.enginesByQualifier = Map.copyOf(engines);
    }

    /**
     * Returns the engine for a store qualifier.
     *
     * @param storeQualifier the qualifier an endpoint resolved to
     * @return the engine backed by that store
     * @throws IllegalStateException if no store has that qualifier
     */
    public IdempotencyEngine engine(String storeQualifier) {
        IdempotencyEngine engine = enginesByQualifier.get(storeQualifier);
        if (engine == null) {
            throw new IllegalStateException("No IdempotencyStore bean found for store qualifier '" + storeQualifier
                    + "' - IdempotentHandlerValidator should have caught this at startup");
        }
        return engine;
    }
}
