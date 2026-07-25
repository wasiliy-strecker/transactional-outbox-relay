package io.github.wasiliystrecker.outbox.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

final class OutboxRelayTest {
  private static final Instant NOW = Instant.parse("2026-07-25T09:00:00Z");

  @Test
  void publishesAndRecordsABrokerAcknowledgement() {
    RecordingStore store = new RecordingStore(claimed(1));
    OutboxRelay relay =
        relay(store, message -> new PublishReceipt("orders.events", 3, 42, NOW.plusSeconds(1)), 5);

    RelayBatchResult result = relay.runOnce("worker-a");

    assertThat(result).isEqualTo(new RelayBatchResult(1, 1, 0, 0, 0));
    assertThat(store.receipt)
        .isEqualTo(new PublishReceipt("orders.events", 3, 42, NOW.plusSeconds(1)));
  }

  @Test
  void reschedulesARetryableFailureWithBackoff() {
    RecordingStore store = new RecordingStore(claimed(2));
    OutboxRelay relay =
        relay(
            store,
            message -> {
              throw new RetryablePublicationException(
                  "broker unavailable", new IllegalStateException("connection reset"));
            },
            5);

    RelayBatchResult result = relay.runOnce("worker-a");

    assertThat(result).isEqualTo(new RelayBatchResult(1, 0, 1, 0, 0));
    assertThat(store.availableAt).isEqualTo(NOW.plusSeconds(4));
    assertThat(store.failure.type()).isEqualTo("publication.retryable");
  }

  @Test
  void marksPermanentAndExhaustedFailuresDead() {
    RecordingStore permanentStore = new RecordingStore(claimed(1));
    OutboxRelay permanentRelay =
        relay(
            permanentStore,
            message -> {
              throw new PermanentPublicationException(
                  "invalid topic", new IllegalArgumentException("bad destination"));
            },
            5);
    RecordingStore exhaustedStore = new RecordingStore(claimed(5));
    OutboxRelay exhaustedRelay =
        relay(
            exhaustedStore,
            message -> {
              throw new RetryablePublicationException(
                  "still unavailable", new IllegalStateException("timeout"));
            },
            5);

    assertThat(permanentRelay.runOnce("worker-a").dead()).isEqualTo(1);
    assertThat(exhaustedRelay.runOnce("worker-b").dead()).isEqualTo(1);
    assertThat(permanentStore.failure.type()).isEqualTo("publication.permanent");
    assertThat(exhaustedStore.failure.type()).isEqualTo("publication.retryable");
  }

  @Test
  void reportsOwnershipLossWithoutOverwritingAnotherWorker() {
    RecordingStore store = new RecordingStore(claimed(1));
    store.acceptTransitions = false;
    OutboxRelay relay =
        relay(store, message -> new PublishReceipt("orders.events", 0, 1, NOW.plusSeconds(1)), 5);

    assertThat(relay.runOnce("worker-a")).isEqualTo(new RelayBatchResult(1, 0, 0, 0, 1));
  }

  @Test
  void leavesTheLeaseForRecoveryWhenPublisherCodeCrashes() {
    RecordingStore store = new RecordingStore(claimed(1));
    OutboxRelay relay =
        relay(
            store,
            message -> {
              throw new IllegalStateException("unexpected serializer bug");
            },
            5);

    assertThatThrownBy(() -> relay.runOnce("worker-a"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("serializer");
    assertThat(store.receipt).isNull();
    assertThat(store.failure).isNull();
  }

  @Test
  void claimsLeaseSafeChunksAndPublishesEachChunkConcurrently() {
    RecordingStore store = new RecordingStore(claimed(1), claimed(1), claimed(1), claimed(1));
    AtomicInteger active = new AtomicInteger();
    AtomicInteger maximumActive = new AtomicInteger();
    OutboxRelay relay =
        new OutboxRelay(
            store,
            message -> {
              int current = active.incrementAndGet();
              maximumActive.accumulateAndGet(current, Math::max);
              try {
                Thread.sleep(Duration.ofMillis(25));
              } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("test publisher interrupted", exception);
              } finally {
                active.decrementAndGet();
              }
              return new PublishReceipt("orders.events", 0, 1, NOW.plusSeconds(1));
            },
            (id, attempt) -> Duration.ofSeconds(1),
            new RelaySettings(4, Duration.ofSeconds(30), 5, 2),
            Clock.fixed(NOW, ZoneOffset.UTC));

    RelayBatchResult result = relay.runOnce("worker-a");

    assertThat(result).isEqualTo(new RelayBatchResult(4, 4, 0, 0, 0));
    assertThat(store.claimSizes).containsExactly(2, 2);
    assertThat(maximumActive).hasValue(2);
  }

  private static OutboxRelay relay(
      RecordingStore store, OutboxPublisher publisher, int maxAttempts) {
    return new OutboxRelay(
        store,
        publisher,
        (id, attempt) -> Duration.ofSeconds(1L << Math.min(attempt, 10)),
        new RelaySettings(10, Duration.ofSeconds(30), maxAttempts),
        Clock.fixed(NOW, ZoneOffset.UTC));
  }

  private static ClaimedOutboxMessage claimed(int attempt) {
    OutboxEventMetadata metadata =
        new OutboxEventMetadata(
            "orders.events",
            "order-42",
            "OrderCreated",
            "v1",
            "Order",
            "order-42",
            1,
            NOW.minusSeconds(1),
            Map.of());
    return new ClaimedOutboxMessage(
        OutboxMessageId.random(),
        metadata,
        "application/json",
        "{}".getBytes(StandardCharsets.UTF_8),
        attempt,
        UUID.randomUUID(),
        NOW.plusSeconds(30),
        NOW.minusSeconds(1));
  }

  private static final class RecordingStore implements OutboxStore {
    private final List<ClaimedOutboxMessage> available;
    private final List<Integer> claimSizes = new ArrayList<>();
    private volatile boolean acceptTransitions = true;
    private volatile PublishReceipt receipt;
    private volatile Instant availableAt;
    private volatile FailureDetails failure;

    private RecordingStore(ClaimedOutboxMessage... available) {
      this.available = new ArrayList<>(List.of(available));
    }

    @Override
    public synchronized List<ClaimedOutboxMessage> claim(ClaimRequest request) {
      int size = Math.min(request.batchSize(), available.size());
      List<ClaimedOutboxMessage> result = List.copyOf(available.subList(0, size));
      available.subList(0, size).clear();
      if (size > 0) {
        claimSizes.add(size);
      }
      return result;
    }

    @Override
    public boolean markPublished(OutboxMessageId id, UUID leaseToken, PublishReceipt receipt) {
      this.receipt = receipt;
      return acceptTransitions;
    }

    @Override
    public boolean reschedule(
        OutboxMessageId id, UUID leaseToken, Instant availableAt, FailureDetails failure) {
      this.availableAt = availableAt;
      this.failure = failure;
      return acceptTransitions;
    }

    @Override
    public boolean markDead(OutboxMessageId id, UUID leaseToken, FailureDetails failure) {
      this.failure = failure;
      return acceptTransitions;
    }

    @Override
    public int deletePublishedBefore(Instant publishedBefore, int limit) {
      return 0;
    }
  }
}
