package io.github.wasiliystrecker.outbox.spring;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Type-safe configuration for the relay, retry policy, payload bound, and cleanup job.
 *
 * @param enabled whether the polling relay starts
 * @param batchSize maximum rows claimed by one poll
 * @param pollInterval fixed delay between completed polls
 * @param leaseDuration exclusive claim duration
 * @param maxAttempts terminal publication attempt
 * @param acknowledgementTimeout maximum Kafka acknowledgement wait
 * @param maxPayloadBytes maximum serialized event size
 * @param workerId optional stable worker name; generated when absent
 * @param retry exponential retry settings
 * @param cleanup published-row cleanup settings
 */
@ConfigurationProperties("outbox.relay")
public record OutboxRelayProperties(
    @DefaultValue("true") boolean enabled,
    @DefaultValue("100") int batchSize,
    @DefaultValue("500ms") Duration pollInterval,
    @DefaultValue("30s") Duration leaseDuration,
    @DefaultValue("10") int maxAttempts,
    @DefaultValue("10s") Duration acknowledgementTimeout,
    @DefaultValue("1048576") int maxPayloadBytes,
    String workerId,
    @DefaultValue Retry retry,
    @DefaultValue Cleanup cleanup) {

  /** Validates all operational bounds after configuration binding. */
  public OutboxRelayProperties {
    range(batchSize, 1, 1_000, "batchSize");
    positive(pollInterval, "pollInterval");
    positive(leaseDuration, "leaseDuration");
    range(maxAttempts, 1, 10_000, "maxAttempts");
    positive(acknowledgementTimeout, "acknowledgementTimeout");
    range(maxPayloadBytes, 1, 100 * 1024 * 1024, "maxPayloadBytes");
    if (workerId != null && workerId.length() > 200) {
      throw new IllegalArgumentException("workerId must not exceed 200 characters");
    }
    if (retry == null) {
      throw new IllegalArgumentException("retry settings must not be null");
    }
    if (cleanup == null) {
      throw new IllegalArgumentException("cleanup settings must not be null");
    }
  }

  /**
   * Exponential retry settings.
   *
   * @param initialDelay first retry delay
   * @param maximumDelay maximum retry delay
   * @param multiplier exponential multiplier
   * @param jitterRatio proportional jitter between zero and one
   */
  public record Retry(
      @DefaultValue("1s") Duration initialDelay,
      @DefaultValue("5m") Duration maximumDelay,
      @DefaultValue("2.0") double multiplier,
      @DefaultValue("0.2") double jitterRatio) {

    /** Validates retry bounds. */
    public Retry {
      positive(initialDelay, "retry.initialDelay");
      positive(maximumDelay, "retry.maximumDelay");
      if (maximumDelay.compareTo(initialDelay) < 0) {
        throw new IllegalArgumentException(
            "retry.maximumDelay must not be shorter than retry.initialDelay");
      }
      if (!Double.isFinite(multiplier) || multiplier < 1.0) {
        throw new IllegalArgumentException("retry.multiplier must be finite and at least 1.0");
      }
      if (!Double.isFinite(jitterRatio) || jitterRatio < 0.0 || jitterRatio > 1.0) {
        throw new IllegalArgumentException("retry.jitterRatio must be between 0.0 and 1.0");
      }
    }
  }

  /**
   * Published-row cleanup settings.
   *
   * @param enabled whether cleanup runs
   * @param retention minimum age of published rows before deletion
   * @param interval minimum duration between cleanup attempts
   * @param batchSize maximum rows deleted per cleanup attempt
   */
  public record Cleanup(
      @DefaultValue("true") boolean enabled,
      @DefaultValue("7d") Duration retention,
      @DefaultValue("1h") Duration interval,
      @DefaultValue("1000") int batchSize) {

    /** Validates cleanup bounds. */
    public Cleanup {
      positive(retention, "cleanup.retention");
      positive(interval, "cleanup.interval");
      range(batchSize, 1, 10_000, "cleanup.batchSize");
    }
  }

  private static void positive(Duration value, String name) {
    if (value == null || value.isZero() || value.isNegative()) {
      throw new IllegalArgumentException(name + " must be positive");
    }
  }

  private static void range(int value, int minimum, int maximum, String name) {
    if (value < minimum || value > maximum) {
      throw new IllegalArgumentException(name + " must be between " + minimum + " and " + maximum);
    }
  }
}
