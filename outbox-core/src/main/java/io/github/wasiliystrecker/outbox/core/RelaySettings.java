package io.github.wasiliystrecker.outbox.core;

import java.time.Duration;

/**
 * Bounded settings for one relay coordinator.
 *
 * @param batchSize maximum messages claimed per poll
 * @param leaseDuration exclusive ownership duration
 * @param maxAttempts terminal attempt limit
 */
public record RelaySettings(int batchSize, Duration leaseDuration, int maxAttempts) {
  /** Validates relay bounds. */
  public RelaySettings {
    if (batchSize < 1 || batchSize > 1_000) {
      throw new IllegalArgumentException("batchSize must be between 1 and 1000");
    }
    leaseDuration = Validation.required(leaseDuration, "leaseDuration");
    if (leaseDuration.isNegative() || leaseDuration.isZero()) {
      throw new IllegalArgumentException("leaseDuration must be positive");
    }
    if (maxAttempts < 1 || maxAttempts > 10_000) {
      throw new IllegalArgumentException("maxAttempts must be between 1 and 10000");
    }
  }
}
