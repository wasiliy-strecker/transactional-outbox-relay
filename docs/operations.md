# Operations

## Database migration

The starter ships its PostgreSQL migration at:

```text
classpath:db/outbox/postgresql
```

Add it to the application's Flyway locations before starting the relay:

```yaml
spring:
  flyway:
    locations:
      - classpath:db/outbox/postgresql
      - classpath:db/migration
```

Treat migration changes like application schema changes: review them, test them against a production-sized copy, and back up the database before upgrades.

## Capacity and lease sizing

`batch-size` is the maximum work budget for one poll. Rows are claimed in chunks no larger than `max-concurrency`, then that chunk is published on virtual threads.

Choose settings using measured broker latency and database pool capacity:

- Keep `max-concurrency` below the connections and Kafka in-flight capacity reserved for relay work.
- Set `lease-duration` comfortably above the Kafka acknowledgement timeout plus a database-transition margin.
- Increase throughput with concurrency and additional application instances, not an unbounded batch.
- Keep `max-payload-bytes` low enough to respect Kafka's producer and broker record limits.

Every application instance can run a worker. `SKIP LOCKED` distributes available rows without a leader election service.

## Metrics and alerting

Useful signals:

| Metric or query | Alert when |
|---|---|
| `outbox.relay.poll.failures` | Any sustained increase |
| `outbox.relay.messages{outcome="dead"}` | Greater than zero |
| `outbox.relay.messages{outcome="ownership_lost"}` | Sustained increase; leases may be too short |
| `outbox.relay.poll.duration` | Approaches the polling or lease budget |
| Oldest pending age | Exceeds the service delivery objective |
| Pending/dead row count | Grows without recovering |

Micrometer counters reset with the process. Database queries provide durable operational truth.

## Diagnostic queries

State distribution:

```sql
SELECT status, count(*)
FROM outbox_message
GROUP BY status
ORDER BY status;
```

Oldest eligible pending event:

```sql
SELECT id, event_type, aggregate_type, aggregate_id, attempt_count,
       available_at, now() - available_at AS eligible_age
FROM outbox_message
WHERE status = 'PENDING'
ORDER BY available_at
LIMIT 20;
```

Expired leases:

```sql
SELECT id, lease_owner, lease_until, attempt_count
FROM outbox_message
WHERE status = 'IN_FLIGHT' AND lease_until < now()
ORDER BY lease_until;
```

Dead messages:

```sql
SELECT id, event_type, aggregate_id, attempt_count,
       last_error_type, last_error_message, last_error_at
FROM outbox_message
WHERE status = 'DEAD'
ORDER BY last_error_at DESC;
```

Do not update lease tokens or statuses during routine diagnosis. Manual requeue operations require a reviewed runbook and an audit trail.

## Cleanup

Cleanup deletes only `PUBLISHED` rows older than `cleanup.retention`, in bounded batches. Retention must cover:

- incident investigation;
- broker/client acknowledgement disputes;
- audit requirements;
- expected consumer replay and reconciliation windows.

Disable automatic cleanup when another archiving process owns retention.

## Deployment checklist

- Apply and verify the Flyway migration.
- Confirm `spring.kafka` authentication, encryption, and broker addresses.
- Confirm the destination topics exist with the required partition and replication policy.
- Ensure the application database user can insert, select, update, and delete `outbox_message`.
- Size the database pool for domain traffic plus relay transitions.
- Set Kafka record-size limits consistently with `max-payload-bytes`.
- Export Micrometer metrics and create dead/backlog alerts.
- Verify clock synchronization across application nodes.
- Exercise process termination during publication in a staging environment.
- Document the consumer deduplication key and retention window.

## Graceful shutdown and crashes

Spring stops the relay lifecycle and interrupts outstanding waits during normal shutdown. An abrupt kill requires no special cleanup: unfinished rows remain `IN_FLIGHT` until their lease expires.

The worker never clears another worker's lease. Every success, retry, and dead transition includes the current random token and checks that the lease is still valid.

## Security

- Payloads are stored in PostgreSQL; classify and protect them like domain data.
- Do not put secrets or personal data into headers.
- Restrict database access to the application and operational roles that need it.
- Use TLS and authentication for PostgreSQL and Kafka outside local development.
- Avoid exposing `last_error_message` directly to untrusted clients.
- Scan dependencies and container images continuously.
