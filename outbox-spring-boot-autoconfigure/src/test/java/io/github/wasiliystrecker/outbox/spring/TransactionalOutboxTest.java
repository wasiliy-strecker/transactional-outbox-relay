package io.github.wasiliystrecker.outbox.spring;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.wasiliystrecker.outbox.core.OutboxEventMetadata;
import io.github.wasiliystrecker.outbox.jdbc.JdbcOutboxWriter;
import java.sql.Connection;
import java.time.Instant;
import java.util.Map;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.AbstractDataSource;
import tools.jackson.databind.json.JsonMapper;

final class TransactionalOutboxTest {
  @Test
  void rejectsWritesOutsideAnApplicationTransaction() {
    var objectMapper = JsonMapper.builder().build();
    JdbcTemplate jdbcTemplate = new JdbcTemplate(failingDataSource());
    JdbcOutboxWriter writer =
        new JdbcOutboxWriter(objectMapper, java.time.Clock.systemUTC(), 1_048_576);
    TransactionalOutbox outbox = new TransactionalOutbox(jdbcTemplate, writer, objectMapper);

    assertThatThrownBy(() -> outbox.appendJson(metadata(), Map.of("orderId", "order-42")))
        .isInstanceOf(NoActiveOutboxTransactionException.class)
        .hasMessageContaining("commit atomically");
  }

  private static OutboxEventMetadata metadata() {
    return new OutboxEventMetadata(
        "orders.events",
        "order-42",
        "OrderCreated",
        "v1",
        "Order",
        "order-42",
        1,
        Instant.parse("2026-07-25T10:00:00Z"),
        Map.of());
  }

  private static DataSource failingDataSource() {
    return new AbstractDataSource() {
      @Override
      public Connection getConnection() {
        throw new AssertionError("No JDBC connection should be requested");
      }

      @Override
      public Connection getConnection(String username, String password) {
        throw new AssertionError("No JDBC connection should be requested");
      }
    };
  }
}
