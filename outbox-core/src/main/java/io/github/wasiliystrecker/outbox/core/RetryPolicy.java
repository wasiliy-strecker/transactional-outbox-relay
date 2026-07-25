package io.github.wasiliystrecker.outbox.core;

import java.time.Duration;

/** Computes the delay before a failed message becomes eligible again. */
@FunctionalInterface
public interface RetryPolicy {
  /**
   * Computes a bounded delay after the current attempt.
   *
   * @param id stable message identity
   * @param attempt one-based failed attempt
   * @return non-negative delay
   */
  Duration delay(OutboxMessageId id, int attempt);
}
