package io.github.wasiliystrecker.outbox.core;

import java.time.Duration;

/** Exponential retry policy with a maximum delay and proportional jitter. */
public final class ExponentialBackoffRetryPolicy implements RetryPolicy {
  private final Duration initialDelay;
  private final Duration maximumDelay;
  private final double multiplier;
  private final double jitterRatio;
  private final JitterSource jitterSource;

  /**
   * Creates a policy with production thread-local jitter.
   *
   * @param initialDelay first retry delay
   * @param maximumDelay maximum retry delay
   * @param multiplier exponential multiplier of at least one
   * @param jitterRatio proportional jitter from zero to one
   */
  public ExponentialBackoffRetryPolicy(
      Duration initialDelay, Duration maximumDelay, double multiplier, double jitterRatio) {
    this(initialDelay, maximumDelay, multiplier, jitterRatio, JitterSource.threadLocal());
  }

  /**
   * Creates a policy with an injectable jitter source.
   *
   * @param initialDelay first retry delay
   * @param maximumDelay maximum retry delay
   * @param multiplier exponential multiplier of at least one
   * @param jitterRatio proportional jitter from zero to one
   * @param jitterSource normalized sample source
   */
  public ExponentialBackoffRetryPolicy(
      Duration initialDelay,
      Duration maximumDelay,
      double multiplier,
      double jitterRatio,
      JitterSource jitterSource) {
    this.initialDelay = positive(initialDelay, "initialDelay");
    this.maximumDelay = positive(maximumDelay, "maximumDelay");
    if (maximumDelay.compareTo(initialDelay) < 0) {
      throw new IllegalArgumentException("maximumDelay must not be shorter than initialDelay");
    }
    if (!Double.isFinite(multiplier) || multiplier < 1.0) {
      throw new IllegalArgumentException("multiplier must be finite and at least 1.0");
    }
    if (!Double.isFinite(jitterRatio) || jitterRatio < 0.0 || jitterRatio > 1.0) {
      throw new IllegalArgumentException("jitterRatio must be between 0.0 and 1.0");
    }
    this.multiplier = multiplier;
    this.jitterRatio = jitterRatio;
    this.jitterSource = Validation.required(jitterSource, "jitterSource");
  }

  @Override
  public Duration delay(OutboxMessageId id, int attempt) {
    Validation.required(id, "id");
    if (attempt < 1) {
      throw new IllegalArgumentException("attempt must be positive");
    }
    double exponentialMillis =
        initialDelay.toMillis() * Math.pow(multiplier, Math.max(0, attempt - 1));
    long baseMillis = (long) Math.min(maximumDelay.toMillis(), Math.max(1.0, exponentialMillis));
    double sample = jitterSource.sample(id, attempt);
    if (!Double.isFinite(sample) || sample < -1.0 || sample > 1.0) {
      throw new IllegalStateException("jitterSource must return a value between -1.0 and 1.0");
    }
    long jitteredMillis = Math.round(baseMillis * (1.0 + (jitterRatio * sample)));
    return Duration.ofMillis(Math.max(1L, Math.min(maximumDelay.toMillis(), jitteredMillis)));
  }

  private static Duration positive(Duration value, String name) {
    Validation.required(value, name);
    if (value.isNegative() || value.isZero()) {
      throw new IllegalArgumentException(name + " must be positive");
    }
    return value;
  }
}
