package io.github.wasiliystrecker.outbox.example.orders;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.wasiliystrecker.outbox.kafka.OutboxKafkaHeaders;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
final class OrderOutboxFlowIT {
  private static final String TOPIC = "orders.events";

  @Container
  private static final PostgreSQLContainer POSTGRES =
      new PostgreSQLContainer("postgres:18.3-alpine");

  @Container
  private static final KafkaContainer KAFKA =
      new KafkaContainer(DockerImageName.parse("apache/kafka:4.2.1"));

  @Autowired private OrderApplicationService orders;
  @Autowired private JdbcTemplate jdbcTemplate;

  @DynamicPropertySource
  static void infrastructure(DynamicPropertyRegistry registry) {
    registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
    registry.add("spring.datasource.username", POSTGRES::getUsername);
    registry.add("spring.datasource.password", POSTGRES::getPassword);
    registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    registry.add("outbox.relay.poll-interval", () -> "50ms");
    registry.add("outbox.relay.cleanup.enabled", () -> "false");
  }

  @Test
  void commitsTheOrderAndPublishesItsDurableEvent() throws Exception {
    createTopic();
    try (KafkaConsumer<String, byte[]> consumer = consumer()) {
      consumer.subscribe(List.of(TOPIC));
      Order order =
          orders.create(new CreateOrderCommand("customer-42", new BigDecimal("149.90"), "EUR"));

      AtomicReference<ConsumerRecord<String, byte[]>> received = new AtomicReference<>();
      await()
          .atMost(20, TimeUnit.SECONDS)
          .pollInterval(Duration.ofMillis(100))
          .untilAsserted(
              () -> {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(250));
                for (ConsumerRecord<String, byte[]> record : records.records(TOPIC)) {
                  if (record.key().equals(order.id().toString())) {
                    received.set(record);
                  }
                }
                assertThat(received.get()).isNotNull();
              });

      ConsumerRecord<String, byte[]> event = received.get();
      assertThat(event.key()).isEqualTo(order.id().toString());
      assertThat(new String(event.value(), UTF_8))
          .contains("\"orderId\":\"" + order.id() + "\"")
          .contains("\"currency\":\"EUR\"");
      assertThat(header(event, OutboxKafkaHeaders.EVENT_TYPE)).isEqualTo("OrderCreated");
      assertThat(header(event, "event-source")).isEqualTo("order-service");

      await()
          .atMost(5, TimeUnit.SECONDS)
          .untilAsserted(
              () ->
                  assertThat(
                          jdbcTemplate.queryForObject(
                              "SELECT status FROM outbox_message WHERE aggregate_id = ?",
                              String.class,
                              order.id().toString()))
                      .isEqualTo("PUBLISHED"));
      assertThat(
              jdbcTemplate.queryForObject(
                  "SELECT count(*) FROM purchase_order WHERE id = ?", Integer.class, order.id()))
          .isEqualTo(1);
    }
  }

  private static void createTopic() throws Exception {
    try (Admin admin =
        Admin.create(
            Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
      admin
          .createTopics(List.of(new NewTopic(TOPIC, 3, (short) 1)))
          .all()
          .get(10, TimeUnit.SECONDS);
    }
  }

  private static KafkaConsumer<String, byte[]> consumer() {
    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "order-flow-" + UUID.randomUUID());
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    return new KafkaConsumer<>(properties);
  }

  private static String header(ConsumerRecord<String, byte[]> record, String name) {
    return new String(record.headers().lastHeader(name).value(), UTF_8);
  }
}
