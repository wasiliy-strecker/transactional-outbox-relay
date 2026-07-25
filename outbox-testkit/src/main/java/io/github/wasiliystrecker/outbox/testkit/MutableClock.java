package io.github.wasiliystrecker.outbox.testkit;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/** Thread-safe manually controlled UTC clock for relay and adapter tests. */
public final class MutableClock extends Clock {
  private final AtomicReference<Instant> current;

  /**
   * Creates a UTC clock at the supplied instant.
   *
   * @param initial initial instant
   */
  public MutableClock(Instant initial) {
    current = new AtomicReference<>(Objects.requireNonNull(initial, "initial"));
  }

  /**
   * Advances time atomically.
   *
   * @param duration positive or negative adjustment
   * @return resulting instant
   */
  public Instant advance(Duration duration) {
    Objects.requireNonNull(duration, "duration");
    return current.updateAndGet(previous -> previous.plus(duration));
  }

  /**
   * Replaces the current instant.
   *
   * @param instant new instant
   */
  public void set(Instant instant) {
    current.set(Objects.requireNonNull(instant, "instant"));
  }

  @Override
  public ZoneId getZone() {
    return ZoneOffset.UTC;
  }

  @Override
  public Clock withZone(ZoneId zone) {
    if (!ZoneOffset.UTC.equals(zone)) {
      throw new IllegalArgumentException("MutableClock supports UTC only");
    }
    return this;
  }

  @Override
  public Instant instant() {
    return current.get();
  }
}
