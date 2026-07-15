package io.adzubla.blocks.idempotency.store;

/** Proves the shared {@link IdempotencyStoreContractTest} suite itself is correct, against the store we already trust. */
class InMemoryIdempotencyStoreContractTest extends IdempotencyStoreContractTest {

    @Override
    protected IdempotencyStore createStore() {
        return new InMemoryIdempotencyStore();
    }
}
