# Delivery semantics

## Contract

The relay provides at-least-once publication when:

- PostgreSQL remains the source of truth for outbox state;
- the application writes through `TransactionalOutbox` in the domain transaction;
- rows are retained until they have reached `PUBLISHED`;
- the relay is allowed to retry or recover expired leases.

At-least-once means consumers must tolerate duplicates. It does not mean every event is delivered in all circumstances: permanently invalid records and retry-exhausted records move to `DEAD` for operator action.

## Failure windows

| Failure point | Durable state | Expected behavior |
|---|---|---|
| Before the domain transaction commits | No domain row and no outbox row | Nothing is published |
| After the joint commit, before claim | `PENDING` | A worker claims it later |
| After claim, before Kafka send | `IN_FLIGHT` | Lease expires and another worker reclaims it |
| Kafka rejects with a retryable error | `IN_FLIGHT` | Row returns to `PENDING` with jittered backoff |
| Kafka rejects with a permanent error | `IN_FLIGHT` | Row moves to `DEAD` |
| Kafka accepted the record but the acknowledgement was lost | Ambiguous | Row may retry; duplicate delivery is possible |
| Process dies after Kafka acknowledgement, before `markPublished` | `IN_FLIGHT` | Lease expires; duplicate delivery is possible |
| Lease expires before the acknowledgement transition | New owner may exist | Old worker's compare-and-set fails; duplicate delivery is possible |
| Database transition fails after send | `IN_FLIGHT` | Lease recovery retries; duplicate delivery is possible |
| Process dies after `markPublished` commits | `PUBLISHED` | No normal retry |

This is why the project does not advertise exactly-once behavior.

## Ordering

The SQL claim uses a deterministic preference order, and a stable aggregate ID is used as Kafka's partition key. Neither creates a global ordering guarantee:

- several relay workers claim concurrently;
- one event can back off while a later event succeeds;
- an expired lease can be reclaimed;
- Kafka only orders accepted records within one partition.

If strict aggregate ordering is a business invariant, add an aggregate sequence check and buffering strategy at the consumer or use a relay mode designed specifically for ordered streams.

## Idempotent consumers

A consumer should use the `outbox.id` header as its delivery identity. A typical database-backed consumer:

1. Starts a local transaction.
2. Inserts `outbox.id` into a table with a unique constraint.
3. If the insert conflicts, treats the message as already handled.
4. Applies the domain change in that same transaction.
5. Commits and only then acknowledges the Kafka record.

Using only the aggregate ID is insufficient because one aggregate emits many valid events. Using only Kafka offset couples deduplication to a topic partition and makes replay behavior harder to express.

Handlers should also validate:

- `outbox.event_type`;
- `outbox.schema_version`;
- expected `outbox.aggregate_type`;
- payload content type;
- domain version rules.

## Retry classification

The Kafka adapter treats broker-retryable errors and acknowledgement timeouts as retryable. Non-retryable Kafka errors are terminal. Unexpected unchecked failures deliberately escape the poll: the current lease remains untouched and is recovered after expiry, mirroring a process crash.

Exponential backoff is capped and jittered to avoid synchronized retry storms. Reaching `max-attempts` moves the row to `DEAD`, even when the last failure was retryable.

## Dead messages

`DEAD` is an operational state, not automatic data loss. The row retains:

- message and aggregate identity;
- payload and headers;
- attempt count;
- last safe error type and message;
- failure timestamp.

Requeueing a dead message is intentionally not exposed as an automatic API in this pre-release. Operators should first correct the configuration or payload cause, preserve an audit trail, and then perform an explicitly reviewed requeue or replacement event.
