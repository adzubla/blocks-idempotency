package io.adzubla.blocks.idempotency.engine;

import io.adzubla.blocks.idempotency.metrics.IdempotencyMetrics;
import io.adzubla.blocks.idempotency.store.IdempotencyStore;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * One {@link IdempotencyEngine} per store qualifier, so a request routes to the
 * engine backed by the store its endpoint resolved (ADR 0001: store is chosen
 * per {@code @Idempotent} endpoint, not once for the whole application). Built
 * once at startup from every {@link IdempotencyStore} bean in the context -
 * {@link io.adzubla.blocks.idempotency.validation.IdempotentHandlerValidator}
 * already guarantees every endpoint's resolved qualifier has a matching store
 * bean, so {@link #engine} throwing signals that invariant was somehow
 * violated, not a normal outcome.
 */
public class IdempotencyEngineRegistry {

    private final Map<String, IdempotencyEngine> enginesByQualifier;

    public IdempotencyEngineRegistry(Map<String, IdempotencyStore> storesByQualifier, IdempotencyMetrics metrics) {
        Map<String, IdempotencyEngine> engines = new LinkedHashMap<>();
        storesByQualifier.forEach((qualifier, store) -> engines.put(qualifier, new IdempotencyEngine(store, metrics)));
        this.enginesByQualifier = Map.copyOf(engines);
    }

    public IdempotencyEngine engine(String storeQualifier) {
        IdempotencyEngine engine = enginesByQualifier.get(storeQualifier);
        if (engine == null) {
            throw new IllegalStateException("No IdempotencyStore bean found for store qualifier '" + storeQualifier
                    + "' - IdempotentHandlerValidator should have caught this at startup");
        }
        return engine;
    }
}
