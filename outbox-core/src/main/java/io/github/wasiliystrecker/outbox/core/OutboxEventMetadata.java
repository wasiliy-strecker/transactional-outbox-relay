package io.github.wasiliystrecker.outbox.core;

import java.time.Instant;
import java.util.Map;

/**
 * Broker routing and domain metadata stored beside an event payload.
 *
 * @param destination broker destination, normally a Kafka topic
 * @param partitionKey stable broker partition key
 * @param eventType business event type
 * @param schemaVersion payload schema version
 * @param aggregateType aggregate category
 * @param aggregateId aggregate identity
 * @param aggregateVersion monotonic domain version
 * @param occurredAt business occurrence timestamp
 * @param headers caller-controlled non-reserved headers
 */
public record OutboxEventMetadata(
    String destination,
    String partitionKey,
    String eventType,
    String schemaVersion,
    String aggregateType,
    String aggregateId,
    long aggregateVersion,
    Instant occurredAt,
    Map<String, String> headers) {

  /** Validates and defensively copies event metadata. */
  public OutboxEventMetadata {
    destination = Validation.text(destination, "destination", 249);
    partitionKey = Validation.text(partitionKey, "partitionKey", 512);
    eventType = Validation.text(eventType, "eventType", 200);
    schemaVersion = Validation.text(schemaVersion, "schemaVersion", 64);
    aggregateType = Validation.text(aggregateType, "aggregateType", 200);
    aggregateId = Validation.text(aggregateId, "aggregateId", 512);
    if (aggregateVersion < 1) {
      throw new IllegalArgumentException("aggregateVersion must be positive");
    }
    occurredAt = Validation.required(occurredAt, "occurredAt");
    headers = Validation.headers(headers);
  }
}
