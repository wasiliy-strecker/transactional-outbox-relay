package io.github.wasiliystrecker.outbox.core;

import java.time.Duration;
import java.time.Instant;

/**
 * One bounded request for messages available to a relay worker.
 *
 * @param workerId diagnostic worker identity
 * @param now claim timestamp
 * @param leaseDuration exclusive ownership duration
 * @param batchSize maximum number of messages
 */
public record ClaimRequest(String workerId, Instant now, Duration leaseDuration, int batchSize) {
  /** Validates claim bounds. */
  public ClaimRequest {
    workerId = Validation.text(workerId, "workerId", 128);
    now = Validation.required(now, "now");
    leaseDuration = Validation.required(leaseDuration, "leaseDuration");
    if (leaseDuration.isZero() || leaseDuration.isNegative()) {
      throw new IllegalArgumentException("leaseDuration must be positive");
    }
    if (batchSize < 1 || batchSize > 1_000) {
      throw new IllegalArgumentException("batchSize must be between 1 and 1000");
    }
  }

  /**
   * Computes the exclusive ownership deadline.
   *
   * @return lease deadline
   */
  public Instant leaseUntil() {
    return now.plus(leaseDuration);
  }
}
