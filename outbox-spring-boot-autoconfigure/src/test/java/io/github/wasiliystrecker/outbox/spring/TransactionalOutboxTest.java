package io.github.wasiliystrecker.outbox.spring;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import io.github.wasiliystrecker.outbox.core.OutboxEventMetadata;
import io.github.wasiliystrecker.outbox.jdbc.JdbcOutboxWriter;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import tools.jackson.databind.json.JsonMapper;

final class TransactionalOutboxTest {
  @Test
  void rejectsWritesOutsideAnApplicationTransaction() {
    JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
    JdbcOutboxWriter writer = mock(JdbcOutboxWriter.class);
    TransactionalOutbox outbox =
        new TransactionalOutbox(jdbcTemplate, writer, JsonMapper.builder().build());

    assertThatThrownBy(() -> outbox.appendJson(metadata(), Map.of("orderId", "order-42")))
        .isInstanceOf(NoActiveOutboxTransactionException.class)
        .hasMessageContaining("commit atomically");
    verifyNoInteractions(jdbcTemplate, writer);
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
}
