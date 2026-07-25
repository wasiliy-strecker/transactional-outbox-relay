package io.github.wasiliystrecker.outbox.kafka;

/**
 * Reserved Kafka header names emitted by {@link KafkaOutboxPublisher}.
 *
 * <p>Application-defined outbox headers cannot use the {@code outbox.} prefix, so these values
 * cannot be overwritten by an event producer.
 */
public final class OutboxKafkaHeaders {
  /** Durable outbox message identity. */
  public static final String MESSAGE_ID = "outbox.id";

  /** Business event type. */
  public static final String EVENT_TYPE = "outbox.event_type";

  /** Payload schema version. */
  public static final String SCHEMA_VERSION = "outbox.schema_version";

  /** Aggregate category. */
  public static final String AGGREGATE_TYPE = "outbox.aggregate_type";

  /** Aggregate identity. */
  public static final String AGGREGATE_ID = "outbox.aggregate_id";

  /** Monotonic aggregate version. */
  public static final String AGGREGATE_VERSION = "outbox.aggregate_version";

  /** Business occurrence timestamp. */
  public static final String OCCURRED_AT = "outbox.occurred_at";

  /** Payload media type. */
  public static final String CONTENT_TYPE = "outbox.content_type";

  /** Current one-based delivery attempt. */
  public static final String ATTEMPT = "outbox.attempt";

  /** Durable row creation timestamp. */
  public static final String CREATED_AT = "outbox.created_at";

  private OutboxKafkaHeaders() {}
}
