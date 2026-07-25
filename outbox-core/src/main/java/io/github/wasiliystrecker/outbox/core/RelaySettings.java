package io.github.wasiliystrecker.outbox.core;

import java.time.Duration;

/**
 * Bounded settings for one relay coordinator.
 *
 * @param batchSize maximum messages claimed per poll
 * @param leaseDuration exclusive ownership duration
 * @param maxAttempts terminal attempt limit
 * @param maxConcurrency maximum messages claimed and processed concurrently
 */
public record RelaySettings(
    int batchSize, Duration leaseDuration, int maxAttempts, int maxConcurrency) {

  /**
   * Creates settings with at most eight concurrent messages.
   *
   * @param batchSize maximum messages claimed per poll
   * @param leaseDuration exclusive ownership duration
   * @param maxAttempts terminal attempt limit
   */
  public RelaySettings(int batchSize, Duration leaseDuration, int maxAttempts) {
    this(batchSize, leaseDuration, maxAttempts, Math.min(batchSize, 8));
  }

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
    if (maxConcurrency < 1 || maxConcurrency > 256) {
      throw new IllegalArgumentException("maxConcurrency must be between 1 and 256");
    }
  }
}
