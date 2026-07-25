package io.github.wasiliystrecker.outbox.core;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/** Coordinates bounded claim chunks, concurrent publication, and durable transitions. */
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
    Counters counters = new Counters();
    while (counters.claimed < settings.batchSize()) {
      int claimSize = Math.min(settings.maxConcurrency(), settings.batchSize() - counters.claimed);
      Instant claimTime = clock.instant();
      List<ClaimedOutboxMessage> messages =
          store.claim(new ClaimRequest(workerId, claimTime, settings.leaseDuration(), claimSize));
      if (messages.size() > claimSize) {
        throw new IllegalStateException(
            "store returned more messages than the requested claim size");
      }
      if (messages.isEmpty()) {
        break;
      }
      counters.claimed += messages.size();
      processConcurrently(messages, counters);
    }
    return counters.result();
  }

  private void processConcurrently(List<ClaimedOutboxMessage> messages, Counters counters) {
    List<Callable<Outcome>> tasks = new ArrayList<>(messages.size());
    for (ClaimedOutboxMessage message : messages) {
      tasks.add(() -> process(message));
    }
    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      List<Future<Outcome>> outcomes = executor.invokeAll(tasks);
      for (Future<Outcome> outcome : outcomes) {
        counters.record(outcome.get());
      }
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while processing an outbox batch", exception);
    } catch (ExecutionException exception) {
      rethrow(exception.getCause());
    }
  }

  private Outcome process(ClaimedOutboxMessage message) {
    try {
      PublishReceipt receipt = publisher.publish(message);
      if (store.markPublished(message.id(), message.leaseToken(), receipt)) {
        return Outcome.PUBLISHED;
      }
      return Outcome.OWNERSHIP_LOST;
    } catch (PublicationException failure) {
      Instant failedAt = clock.instant();
      FailureDetails details = failureDetails(failure, failedAt);
      if (!failure.retryable() || message.attempt() >= settings.maxAttempts()) {
        if (store.markDead(message.id(), message.leaseToken(), details)) {
          return Outcome.DEAD;
        }
        return Outcome.OWNERSHIP_LOST;
      }
      Duration delay = retryPolicy.delay(message.id(), message.attempt());
      if (delay.isNegative()) {
        throw new IllegalStateException("retryPolicy returned a negative delay");
      }
      if (store.reschedule(message.id(), message.leaseToken(), failedAt.plus(delay), details)) {
        return Outcome.RESCHEDULED;
      }
      return Outcome.OWNERSHIP_LOST;
    }
  }

  private static void rethrow(Throwable cause) {
    if (cause instanceof RuntimeException runtimeException) {
      throw runtimeException;
    }
    if (cause instanceof Error error) {
      throw error;
    }
    throw new IllegalStateException("Outbox worker failed with a checked exception", cause);
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
    private int claimed;
    private int published;
    private int rescheduled;
    private int dead;
    private int ownershipLost;

    private void record(Outcome outcome) {
      switch (outcome) {
        case PUBLISHED -> published++;
        case RESCHEDULED -> rescheduled++;
        case DEAD -> dead++;
        case OWNERSHIP_LOST -> ownershipLost++;
      }
    }

    private RelayBatchResult result() {
      return new RelayBatchResult(claimed, published, rescheduled, dead, ownershipLost);
    }
  }

  private enum Outcome {
    PUBLISHED,
    RESCHEDULED,
    DEAD,
    OWNERSHIP_LOST
  }
}
