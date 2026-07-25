package io.github.wasiliystrecker.outbox.core;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

/** Coordinates one bounded claim, publish, and durable transition cycle. */
public final class OutboxRelay {
  private final OutboxStore store;
  private final OutboxPublisher publisher;
  private final RetryPolicy retryPolicy;
  private final RelaySettings settings;
  private final Clock clock;

  /**
   * Creates a relay coordinator.
   *
   * @param store durable ownership boundary
   * @param publisher broker boundary
   * @param retryPolicy retry delay policy
   * @param settings claim and attempt bounds
   * @param clock time source
   */
  public OutboxRelay(
      OutboxStore store,
      OutboxPublisher publisher,
      RetryPolicy retryPolicy,
      RelaySettings settings,
      Clock clock) {
    this.store = Validation.required(store, "store");
    this.publisher = Validation.required(publisher, "publisher");
    this.retryPolicy = Validation.required(retryPolicy, "retryPolicy");
    this.settings = Validation.required(settings, "settings");
    this.clock = Validation.required(clock, "clock");
  }

  /**
   * Processes at most one configured batch.
   *
   * <p>Unchecked publisher or store failures deliberately escape. The claimed row remains leased
   * and becomes eligible for recovery after lease expiry, matching a process crash.
   *
   * @param workerId stable diagnostic identity
   * @return bounded outcome counters
   */
  public RelayBatchResult runOnce(String workerId) {
    Instant claimTime = clock.instant();
    List<ClaimedOutboxMessage> messages =
        store.claim(
            new ClaimRequest(workerId, claimTime, settings.leaseDuration(), settings.batchSize()));
    Counters counters = new Counters(messages.size());
    for (ClaimedOutboxMessage message : messages) {
      process(message, counters);
    }
    return counters.result();
  }

  private void process(ClaimedOutboxMessage message, Counters counters) {
    try {
      PublishReceipt receipt = publisher.publish(message);
      if (store.markPublished(message.id(), message.leaseToken(), receipt)) {
        counters.published++;
      } else {
        counters.ownershipLost++;
      }
    } catch (PublicationException failure) {
      Instant failedAt = clock.instant();
      FailureDetails details = failureDetails(failure, failedAt);
      if (!failure.retryable() || message.attempt() >= settings.maxAttempts()) {
        if (store.markDead(message.id(), message.leaseToken(), details)) {
          counters.dead++;
        } else {
          counters.ownershipLost++;
        }
        return;
      }
      Duration delay = retryPolicy.delay(message.id(), message.attempt());
      if (delay.isNegative()) {
        throw new IllegalStateException("retryPolicy returned a negative delay");
      }
      if (store.reschedule(message.id(), message.leaseToken(), failedAt.plus(delay), details)) {
        counters.rescheduled++;
      } else {
        counters.ownershipLost++;
      }
    }
  }

  private static FailureDetails failureDetails(PublicationException failure, Instant failedAt) {
    String type = failure.retryable() ? "publication.retryable" : "publication.permanent";
    String message = failure.getMessage();
    if (message == null || message.isBlank()) {
      message = failure.getClass().getSimpleName();
    }
    if (message.length() > 1_024) {
      message = message.substring(0, 1_024);
    }
    return new FailureDetails(type, message, failedAt);
  }

  private static final class Counters {
    private final int claimed;
    private int published;
    private int rescheduled;
    private int dead;
    private int ownershipLost;

    private Counters(int claimed) {
      this.claimed = claimed;
    }

    private RelayBatchResult result() {
      return new RelayBatchResult(claimed, published, rescheduled, dead, ownershipLost);
    }
  }
}
