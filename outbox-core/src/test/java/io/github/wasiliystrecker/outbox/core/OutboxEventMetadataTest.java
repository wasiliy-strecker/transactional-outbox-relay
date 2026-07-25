package io.github.wasiliystrecker.outbox.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

final class OutboxEventMetadataTest {
  @Test
  void copiesHeadersAndRejectsReservedNames() {
    Map<String, String> headers = new LinkedHashMap<>();
    headers.put("trace-id", "trace-42");

    OutboxEventMetadata metadata = metadata(headers);
    headers.put("tenant", "changed-after-construction");

    assertThat(metadata.headers()).containsExactly(Map.entry("trace-id", "trace-42"));
    assertThatIllegalArgumentException()
        .isThrownBy(() -> metadata(Map.of("Outbox.custom", "forbidden")))
        .withMessageContaining("reserved");
  }

  @Test
  void requiresAPositiveAggregateVersion() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new OutboxEventMetadata(
                    "orders.events",
                    "order-42",
                    "OrderCreated",
                    "v1",
                    "Order",
                    "order-42",
                    0,
                    Instant.parse("2026-07-25T08:00:00Z"),
                    Map.of()))
        .withMessageContaining("aggregateVersion");
  }

  private static OutboxEventMetadata metadata(Map<String, String> headers) {
    return new OutboxEventMetadata(
        "orders.events",
        "order-42",
        "OrderCreated",
        "v1",
        "Order",
        "order-42",
        1,
        Instant.parse("2026-07-25T08:00:00Z"),
        headers);
  }
}
