package io.github.wasiliystrecker.outbox.kafka;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.wasiliystrecker.outbox.core.ClaimedOutboxMessage;
import io.github.wasiliystrecker.outbox.core.OutboxEventMetadata;
import io.github.wasiliystrecker.outbox.core.OutboxMessageId;
import io.github.wasiliystrecker.outbox.core.PermanentPublicationException;
import io.github.wasiliystrecker.outbox.core.PublishReceipt;
import io.github.wasiliystrecker.outbox.core.RetryablePublicationException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

final class KafkaOutboxPublisherTest {
  private static final Instant PUBLISHED_AT = Instant.parse("2026-07-25T10:15:30Z");

  @Test
  void mapsRoutingPayloadAndMetadataAndReturnsBrokerReceipt() throws Exception {
    MockProducer<String, byte[]> producer = producer(true);
    KafkaOutboxPublisher publisher =
        new KafkaOutboxPublisher(
            producer, Duration.ofSeconds(2), Clock.fixed(PUBLISHED_AT, ZoneOffset.UTC));

    PublishReceipt receipt = publisher.publish(message());

    assertThat(receipt.destination()).isEqualTo("orders.events");
    assertThat(receipt.partition()).isZero();
    assertThat(receipt.offset()).isZero();
    assertThat(receipt.publishedAt()).isEqualTo(PUBLISHED_AT);

    assertThat(producer.history()).hasSize(1);
    ProducerRecord<String, byte[]> record = producer.history().getFirst();
    assertThat(record.topic()).isEqualTo("orders.events");
    assertThat(record.key()).isEqualTo("order-42");
    assertThat(record.value()).containsExactly("{\"orderId\":\"order-42\"}".getBytes(UTF_8));
    assertThat(header(record, "trace-id")).isEqualTo("trace-42");
    assertThat(header(record, OutboxKafkaHeaders.MESSAGE_ID))
        .isEqualTo("018f47ec-72c2-7a62-8149-9c32c2892a10");
    assertThat(header(record, OutboxKafkaHeaders.EVENT_TYPE)).isEqualTo("OrderCreated");
    assertThat(header(record, OutboxKafkaHeaders.SCHEMA_VERSION)).isEqualTo("v1");
    assertThat(header(record, OutboxKafkaHeaders.AGGREGATE_TYPE)).isEqualTo("Order");
    assertThat(header(record, OutboxKafkaHeaders.AGGREGATE_ID)).isEqualTo("order-42");
    assertThat(header(record, OutboxKafkaHeaders.AGGREGATE_VERSION)).isEqualTo("3");
    assertThat(header(record, OutboxKafkaHeaders.OCCURRED_AT)).isEqualTo("2026-07-25T10:00:00Z");
    assertThat(header(record, OutboxKafkaHeaders.CONTENT_TYPE)).isEqualTo("application/json");
    assertThat(header(record, OutboxKafkaHeaders.ATTEMPT)).isEqualTo("2");
    assertThat(header(record, OutboxKafkaHeaders.CREATED_AT)).isEqualTo("2026-07-25T10:00:01Z");
  }

  @Test
  void classifiesRetriableBrokerErrors() {
    KafkaOutboxPublisher publisher =
        new KafkaOutboxPublisher(
            producerFailingWith(new NetworkException("broker unavailable")), Duration.ofSeconds(2));

    assertThatThrownBy(() -> publisher.publish(message()))
        .isInstanceOf(RetryablePublicationException.class)
        .hasCauseInstanceOf(NetworkException.class);
  }

  @Test
  void classifiesPermanentProducerErrors() {
    KafkaOutboxPublisher publisher =
        new KafkaOutboxPublisher(
            producerFailingWith(new SerializationException("unsupported payload")),
            Duration.ofSeconds(2));

    assertThatThrownBy(() -> publisher.publish(message()))
        .isInstanceOf(PermanentPublicationException.class)
        .hasCauseInstanceOf(SerializationException.class);
  }

  @Test
  void timesOutWhenKafkaDoesNotAcknowledgeTheRecord() {
    MockProducer<String, byte[]> producer = producer(false);
    KafkaOutboxPublisher publisher = new KafkaOutboxPublisher(producer, Duration.ofMillis(1));

    assertThatThrownBy(() -> publisher.publish(message()))
        .isInstanceOf(RetryablePublicationException.class)
        .hasMessageContaining("timed out");
  }

  @Test
  void requiresAPositiveAcknowledgementTimeout() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new KafkaOutboxPublisher(producer(true), Duration.ZERO))
        .withMessageContaining("positive");
  }

  private static MockProducer<String, byte[]> producer(boolean autoComplete) {
    return new MockProducer<String, byte[]>(
        autoComplete, null, new StringSerializer(), new ByteArraySerializer());
  }

  private static MockProducer<String, byte[]> producerFailingWith(RuntimeException failure) {
    return new MockProducer<String, byte[]>(
        true, null, new StringSerializer(), new ByteArraySerializer()) {
      @Override
      public synchronized Future<RecordMetadata> send(ProducerRecord<String, byte[]> record) {
        return CompletableFuture.failedFuture(failure);
      }
    };
  }

  private static ClaimedOutboxMessage message() {
    OutboxEventMetadata metadata =
        new OutboxEventMetadata(
            "orders.events",
            "order-42",
            "OrderCreated",
            "v1",
            "Order",
            "order-42",
            3,
            Instant.parse("2026-07-25T10:00:00Z"),
            Map.of("trace-id", "trace-42"));
    return new ClaimedOutboxMessage(
        new OutboxMessageId(UUID.fromString("018f47ec-72c2-7a62-8149-9c32c2892a10")),
        metadata,
        "application/json",
        "{\"orderId\":\"order-42\"}".getBytes(UTF_8),
        2,
        UUID.fromString("f232ca21-c401-4b3a-8e2d-7d9732b30175"),
        Instant.parse("2026-07-25T10:00:30Z"),
        Instant.parse("2026-07-25T10:00:01Z"));
  }

  private static String header(ProducerRecord<String, byte[]> record, String name) {
    return new String(record.headers().lastHeader(name).value(), UTF_8);
  }
}
