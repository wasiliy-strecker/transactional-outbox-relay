package io.github.wasiliystrecker.outbox.core;

import java.util.concurrent.ThreadLocalRandom;

/** Supplies a normalized jitter sample for retry spreading. */
@FunctionalInterface
public interface JitterSource {
  /**
   * Returns a sample in the inclusive range from {@code -1.0} to {@code 1.0}.
   *
   * @param id stable message identity
   * @param attempt one-based attempt
   * @return normalized sample
   */
  double sample(OutboxMessageId id, int attempt);

  /**
   * Uses thread-local randomness for production retry spreading.
   *
   * @return random jitter source
   */
  static JitterSource threadLocal() {
    return (id, attempt) -> ThreadLocalRandom.current().nextDouble(-1.0, 1.0);
  }
}
