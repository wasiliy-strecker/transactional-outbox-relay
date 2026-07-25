package io.github.wasiliystrecker.outbox.testkit;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.wasiliystrecker.outbox.core.ClaimRequest;
import io.github.wasiliystrecker.outbox.core.ClaimedOutboxMessage;
import io.github.wasiliystrecker.outbox.core.FailureDetails;
import io.github.wasiliystrecker.outbox.core.NewOutboxMessage;
import io.github.wasiliystrecker.outbox.core.OutboxEventMetadata;
import io.github.wasiliystrecker.outbox.core.OutboxMessageId;
import io.github.wasiliystrecker.outbox.core.OutboxStatus;
import io.github.wasiliystrecker.outbox.core.OutboxStore;
import io.github.wasiliystrecker.outbox.core.PublishReceipt;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Shared atomicity and ownership contract for every durable outbox store. */
public abstract class OutboxStoreContract {
  private static final Instant START = Instant.parse("2026-07-25T08:00:00Z");

  /** Creates the contract. */
  protected OutboxStoreContract() {}

  /**
   * Supplies the store under test.
   *
   * @return store
   */
  protected abstract OutboxStore store();

  /**
   * Supplies persistence-specific setup and inspection.
   *
   * @return harness
   */
  protected abstract OutboxStoreHarness harness();

  /** Resets durable state before each contract example. */
  @BeforeEach
  final void resetStore() {
    harness().reset();
  }

  /** Verifies exclusive claims and immutable message reconstruction. */
  @Test
  final void claimsAnAvailableMessageOnlyOnce() {
    OutboxMessageId id = harness().append(message("order-1"), START);

    List<ClaimedOutboxMessage> first = store().claim(claim("worker-a", START, 10));
    List<ClaimedOutboxMessage> second = store().claim(claim("worker-b", START, 10));

    assertThat(first).hasSize(1);
    assertThat(first.getFirst().id()).isEqualTo(id);
    assertThat(first.getFirst().attempt()).isEqualTo(1);
    assertThat(first.getFirst().payload())
        .isEqualTo("{\"orderId\":\"order-1\"}".getBytes(StandardCharsets.UTF_8));
    assertThat(second).isEmpty();
    assertThat(harness().status(id)).isEqualTo(OutboxStatus.IN_FLIGHT);
  }

  /** Verifies that an expired lease can be reclaimed with a new token. */
  @Test
  final void reclaimsExpiredWork() {
    OutboxMessageId id = harness().append(message("order-2"), START);
    ClaimedOutboxMessage first = store().claim(claim("worker-a", START, 1)).getFirst();

    ClaimedOutboxMessage recovered =
        store().claim(claim("worker-b", START.plusSeconds(31), 1)).getFirst();

    assertThat(recovered.id()).isEqualTo(id);
    assertThat(recovered.attempt()).isEqualTo(2);
    assertThat(recovered.leaseToken()).isNotEqualTo(first.leaseToken());
  }

  /** Verifies compare-and-set ownership for successful publication. */
  @Test
  final void acceptsOnlyTheCurrentLeaseToken() {
    OutboxMessageId id = harness().append(message("order-3"), START);
    ClaimedOutboxMessage claimed = store().claim(claim("worker-a", START, 1)).getFirst();
    PublishReceipt receipt = new PublishReceipt("orders.events", 2, 41, START.plusSeconds(1));

    assertThat(store().markPublished(id, UUID.randomUUID(), receipt)).isFalse();
    assertThat(store().markPublished(id, claimed.leaseToken(), receipt)).isTrue();
    assertThat(store().markPublished(id, claimed.leaseToken(), receipt)).isFalse();
    assertThat(harness().status(id)).isEqualTo(OutboxStatus.PUBLISHED);
  }

  /** Verifies retry scheduling and terminal dead transitions. */
  @Test
  final void reschedulesAndMarksDead() {
    OutboxMessageId retryId = harness().append(message("order-4"), START);
    ClaimedOutboxMessage retry = store().claim(claim("worker-a", START, 1)).getFirst();
    FailureDetails failure =
        new FailureDetails("broker-unavailable", "Kafka did not acknowledge", START.plusSeconds(1));

    assertThat(store().reschedule(retryId, retry.leaseToken(), START.plusSeconds(60), failure))
        .isTrue();
    assertThat(store().claim(claim("worker-b", START.plusSeconds(59), 10))).isEmpty();
    ClaimedOutboxMessage retried =
        store().claim(claim("worker-b", START.plusSeconds(60), 10)).getFirst();
    assertThat(retried.attempt()).isEqualTo(2);

    assertThat(store().markDead(retryId, retried.leaseToken(), failure)).isTrue();
    assertThat(harness().status(retryId)).isEqualTo(OutboxStatus.DEAD);
  }

  /** Verifies bounded cleanup of acknowledged messages. */
  @Test
  final void deletesOnlyPublishedRowsBeforeTheCutoff() {
    OutboxMessageId publishedId = harness().append(message("order-5"), START);
    harness().append(message("order-6"), START.plusSeconds(1));
    ClaimedOutboxMessage claimed = store().claim(claim("worker-a", START, 1)).getFirst();
    store()
        .markPublished(
            publishedId,
            claimed.leaseToken(),
            new PublishReceipt("orders.events", 0, 1, START.plusSeconds(1)));

    assertThat(store().deletePublishedBefore(START.plusSeconds(2), 10)).isEqualTo(1);
    assertThat(harness().status(publishedId)).isNull();
  }

  private static ClaimRequest claim(String worker, Instant now, int batchSize) {
    return new ClaimRequest(worker, now, Duration.ofSeconds(30), batchSize);
  }

  private static NewOutboxMessage message(String orderId) {
    OutboxEventMetadata metadata =
        new OutboxEventMetadata(
            "orders.events",
            orderId,
            "OrderCreated",
            "v1",
            "Order",
            orderId,
            1,
            START,
            Map.of("trace-id", "trace-" + orderId));
    return new NewOutboxMessage(
        metadata,
        "application/json",
        ("{\"orderId\":\"" + orderId + "\"}").getBytes(StandardCharsets.UTF_8));
  }
}
