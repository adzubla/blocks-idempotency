-- Test-only "business effect" table: proves a handler's own DB write shares
-- the reservation's transaction (ADR 0003), not part of the library schema.
CREATE TABLE test_orders (
    id     SERIAL PRIMARY KEY,
    amount INT NOT NULL
);
