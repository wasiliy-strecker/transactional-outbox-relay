package io.github.wasiliystrecker.outbox.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

final class MessageValueObjectsTest {
  @Test
  void protectsNewAndClaimedPayloadsFromMutation() {
    byte[] source = {1, 2, 3};
    NewOutboxMessage fresh = new NewOutboxMessage(metadata(), "application/json", source);
    source[0] = 9;
    byte[] returned = fresh.payload();
    returned[1] = 9;

    ClaimedOutboxMessage claimed =
        new ClaimedOutboxMessage(
            OutboxMessageId.random(),
            metadata(),
            "application/json",
            fresh.payload(),
            1,
            UUID.randomUUID(),
            Instant.parse("2026-07-25T08:00:30Z"),
            Instant.parse("2026-07-25T08:00:00Z"));
    byte[] claimedPayload = claimed.payload();
    claimedPayload[2] = 9;

    assertThat(fresh.payload()).containsExactly(1, 2, 3);
    assertThat(claimed.payload()).containsExactly(1, 2, 3);
  }

  @Test
  void validatesClaimBounds() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new ClaimRequest(
                    "worker-a", Instant.parse("2026-07-25T08:00:00Z"), Duration.ZERO, 10))
        .withMessageContaining("leaseDuration");
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new ClaimRequest(
                    "worker-a",
                    Instant.parse("2026-07-25T08:00:00Z"),
                    Duration.ofSeconds(30),
                    1_001))
        .withMessageContaining("batchSize");
  }

  @Test
  void computesTheLeaseDeadlineFromAnImmutableRequest() {
    ClaimRequest request =
        new ClaimRequest(
            "worker-a", Instant.parse("2026-07-25T08:00:00Z"), Duration.ofSeconds(30), 100);

    assertThat(request.leaseUntil()).isEqualTo(Instant.parse("2026-07-25T08:00:30Z"));
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
        Instant.parse("2026-07-25T08:00:00Z"),
        Map.of("trace-id", "trace-42"));
  }
}
