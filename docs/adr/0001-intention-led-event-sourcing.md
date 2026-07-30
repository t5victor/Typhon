# ADR 0001: event sourcing for competitive allocation

## Context

Auctions create the kinds of conflicts that are easy to hide behind mutable
rows: two bidders can race for the same lot, a payment can arrive late and a
read model can lag behind the accepted decision. The history matters when that
happens.

## Decision

Record append-only business facts. Commands name the decision being requested;
events name the fact that occurred. Rehydrate aggregates from their complete
stream and keep projections outside the decision path. Thyphon does not use
snapshots.

## Consequences

The audit trail can be replayed and read models can be rebuilt. PostgreSQL
enforces optimistic stream versions while the outbox carries accepted facts to
Kafka. Rehydration cost grows with stream length; that cost is visible and will
be measured before introducing a different design.
