# ADR 0001: use intention-led event sourcing for competitive allocation

## Decision

Thyphon records append-only domain facts. Commands are named for a business decision and events for a business fact. No snapshots are used: every aggregate is rehydrated from its complete stream.

## Consequences

The audit trail is direct and projection rebuilds are trustworthy. Long-running streams will eventually make rehydration costly; this is an accepted, measurable constraint of Delivery 1 rather than a hidden optimization. Command handlers enforce optimistic versions and never read projections to decide legality.
