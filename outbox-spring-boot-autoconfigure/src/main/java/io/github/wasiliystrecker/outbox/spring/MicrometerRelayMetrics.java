package io.github.wasiliystrecker.outbox.spring;

import io.github.wasiliystrecker.outbox.core.RelayBatchResult;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;

final class MicrometerRelayMetrics implements RelayMetrics {
  private final Counter claimed;
  private final Counter published;
  private final Counter rescheduled;
  private final Counter dead;
  private final Counter ownershipLost;
  private final Counter pollFailures;
  private final Counter deleted;
  private final Timer pollDuration;

  MicrometerRelayMetrics(MeterRegistry registry) {
    claimed = counter(registry, "claimed");
    published = counter(registry, "published");
    rescheduled = counter(registry, "rescheduled");
    dead = counter(registry, "dead");
    ownershipLost = counter(registry, "ownership_lost");
    pollFailures = Counter.builder("outbox.relay.poll.failures").register(registry);
    deleted = Counter.builder("outbox.relay.cleanup.deleted").register(registry);
    pollDuration = Timer.builder("outbox.relay.poll.duration").register(registry);
  }

  @Override
  public void recordBatch(RelayBatchResult result, Duration duration) {
    claimed.increment(result.claimed());
    published.increment(result.published());
    rescheduled.increment(result.rescheduled());
    dead.increment(result.dead());
    ownershipLost.increment(result.ownershipLost());
    pollDuration.record(duration);
  }

  @Override
  public void recordFailure(Duration duration) {
    pollFailures.increment();
    pollDuration.record(duration);
  }

  @Override
  public void recordDeleted(int count) {
    deleted.increment(count);
  }

  private static Counter counter(MeterRegistry registry, String outcome) {
    return Counter.builder("outbox.relay.messages").tag("outcome", outcome).register(registry);
  }
}
