package io.github.wasiliystrecker.outbox.kafka;

import io.github.wasiliystrecker.outbox.core.ClaimedOutboxMessage;
import io.github.wasiliystrecker.outbox.core.OutboxEventMetadata;
import io.github.wasiliystrecker.outbox.core.OutboxPublisher;
import io.github.wasiliystrecker.outbox.core.PermanentPublicationException;
import io.github.wasiliystrecker.outbox.core.PublicationException;
import io.github.wasiliystrecker.outbox.core.PublishReceipt;
import io.github.wasiliystrecker.outbox.core.RetryablePublicationException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.header.internals.RecordHeaders;

/**
 * Publishes claimed outbox messages through a caller-owned Kafka producer.
 *
 * <p>The publisher waits for the broker acknowledgement before returning. It does not flush or
 * close the producer because its lifecycle remains with the caller.
 */
public final class KafkaOutboxPublisher implements OutboxPublisher {
  private final Producer<String, byte[]> producer;
  private final Duration acknowledgementTimeout;
  private final Clock clock;

  /**
   * Creates a publisher using the system UTC clock.
   *
   * @param producer caller-owned Kafka producer
   * @param acknowledgementTimeout maximum wait for a broker acknowledgement
   */
  public KafkaOutboxPublisher(Producer<String, byte[]> producer, Duration acknowledgementTimeout) {
    this(producer, acknowledgementTimeout, Clock.systemUTC());
  }

  /**
   * Creates a publisher with an injectable clock.
   *
   * @param producer caller-owned Kafka producer
   * @param acknowledgementTimeout maximum wait for a broker acknowledgement
   * @param clock acknowledgement clock
   */
  public KafkaOutboxPublisher(
      Producer<String, byte[]> producer, Duration acknowledgementTimeout, Clock clock) {
    this.producer = Objects.requireNonNull(producer, "producer must not be null");
    this.acknowledgementTimeout = positive(acknowledgementTimeout);
    this.clock = Objects.requireNonNull(clock, "clock must not be null");
  }

  @Override
  public PublishReceipt publish(ClaimedOutboxMessage message) throws PublicationException {
    Objects.requireNonNull(message, "message must not be null");
    ProducerRecord<String, byte[]> record = toProducerRecord(message);

    try {
      RecordMetadata acknowledgement =
          producer.send(record).get(acknowledgementTimeout.toNanos(), TimeUnit.NANOSECONDS);
      return new PublishReceipt(
          acknowledgement.topic(),
          acknowledgement.partition(),
          acknowledgement.offset(),
          clock.instant());
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      throw new RetryablePublicationException(
          "Interrupted while waiting for Kafka acknowledgement", exception);
    } catch (TimeoutException exception) {
      throw new RetryablePublicationException(
          "Kafka acknowledgement timed out after " + acknowledgementTimeout, exception);
    } catch (ExecutionException exception) {
      throw classify(exception.getCause());
    } catch (RetriableException exception) {
      throw retryable(exception);
    } catch (KafkaException exception) {
      throw permanent(exception);
    }
  }

  private static ProducerRecord<String, byte[]> toProducerRecord(ClaimedOutboxMessage message) {
    OutboxEventMetadata metadata = message.metadata();
    RecordHeaders headers = new RecordHeaders();
    metadata.headers().forEach((name, value) -> add(headers, name, value));
    add(headers, OutboxKafkaHeaders.MESSAGE_ID, message.id().toString());
    add(headers, OutboxKafkaHeaders.EVENT_TYPE, metadata.eventType());
    add(headers, OutboxKafkaHeaders.SCHEMA_VERSION, metadata.schemaVersion());
    add(headers, OutboxKafkaHeaders.AGGREGATE_TYPE, metadata.aggregateType());
    add(headers, OutboxKafkaHeaders.AGGREGATE_ID, metadata.aggregateId());
    add(headers, OutboxKafkaHeaders.AGGREGATE_VERSION, Long.toString(metadata.aggregateVersion()));
    add(headers, OutboxKafkaHeaders.OCCURRED_AT, metadata.occurredAt().toString());
    add(headers, OutboxKafkaHeaders.CONTENT_TYPE, message.contentType());
    add(headers, OutboxKafkaHeaders.ATTEMPT, Integer.toString(message.attempt()));
    add(headers, OutboxKafkaHeaders.CREATED_AT, message.createdAt().toString());
    return new ProducerRecord<>(
        metadata.destination(), null, null, metadata.partitionKey(), message.payload(), headers);
  }

  private static PublicationException classify(Throwable cause) {
    if (cause instanceof RetriableException retryable) {
      return retryable(retryable);
    }
    if (cause instanceof KafkaException kafkaException) {
      return permanent(kafkaException);
    }
    return new PermanentPublicationException(
        "Kafka publication failed with an unexpected acknowledgement error", cause);
  }

  private static RetryablePublicationException retryable(KafkaException cause) {
    return new RetryablePublicationException(
        "Kafka publication failed with a retryable broker error", cause);
  }

  private static PermanentPublicationException permanent(KafkaException cause) {
    return new PermanentPublicationException(
        "Kafka publication failed with a non-retryable producer error", cause);
  }

  private static void add(RecordHeaders headers, String name, String value) {
    headers.add(name, value.getBytes(StandardCharsets.UTF_8));
  }

  private static Duration positive(Duration timeout) {
    Objects.requireNonNull(timeout, "acknowledgementTimeout must not be null");
    if (timeout.isZero() || timeout.isNegative()) {
      throw new IllegalArgumentException("acknowledgementTimeout must be positive");
    }
    return timeout;
  }
}
