package io.github.wasiliystrecker.outbox.spring;

import io.github.wasiliystrecker.outbox.core.OutboxRelay;
import io.github.wasiliystrecker.outbox.core.OutboxStore;
import io.github.wasiliystrecker.outbox.core.RelayBatchResult;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

final class OutboxRelayLifecycle implements SmartLifecycle {
  private static final Logger LOGGER = LoggerFactory.getLogger(OutboxRelayLifecycle.class);

  private final OutboxRelay relay;
  private final OutboxStore store;
  private final RelayMetrics metrics;
  private final OutboxRelayProperties properties;
  private final Clock clock;
  private final String workerId;

  private volatile boolean running;
  private volatile Instant nextCleanup = Instant.EPOCH;
  private ScheduledExecutorService executor;

  OutboxRelayLifecycle(
      OutboxRelay relay,
      OutboxStore store,
      RelayMetrics metrics,
      OutboxRelayProperties properties,
      Clock clock,
      String workerId) {
    this.relay = Objects.requireNonNull(relay, "relay");
    this.store = Objects.requireNonNull(store, "store");
    this.metrics = Objects.requireNonNull(metrics, "metrics");
    this.properties = Objects.requireNonNull(properties, "properties");
    this.clock = Objects.requireNonNull(clock, "clock");
    this.workerId = Objects.requireNonNull(workerId, "workerId");
  }

  @Override
  public synchronized void start() {
    if (running) {
      return;
    }
    executor =
        Executors.newSingleThreadScheduledExecutor(
            Thread.ofPlatform().daemon().name("outbox-relay-", 0).factory());
    running = true;
    executor.scheduleWithFixedDelay(
        this::pollSafely, 0L, properties.pollInterval().toNanos(), TimeUnit.NANOSECONDS);
    LOGGER.info("Started transactional outbox relay worker {}", workerId);
  }

  @Override
  public synchronized void stop() {
    if (!running) {
      return;
    }
    running = false;
    executor.shutdownNow();
    executor = null;
    LOGGER.info("Stopped transactional outbox relay worker {}", workerId);
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  private void pollSafely() {
    long started = System.nanoTime();
    try {
      RelayBatchResult result = relay.runOnce(workerId);
      metrics.recordBatch(result, elapsedSince(started));
    } catch (RuntimeException failure) {
      metrics.recordFailure(elapsedSince(started));
      LOGGER.warn("Transactional outbox relay poll failed; the next poll will retry", failure);
    }
    cleanupSafely();
  }

  private void cleanupSafely() {
    OutboxRelayProperties.Cleanup cleanup = properties.cleanup();
    Instant now = clock.instant();
    if (!cleanup.enabled() || now.isBefore(nextCleanup)) {
      return;
    }
    nextCleanup = now.plus(cleanup.interval());
    try {
      int deleted =
          store.deletePublishedBefore(now.minus(cleanup.retention()), cleanup.batchSize());
      metrics.recordDeleted(deleted);
      if (deleted > 0) {
        LOGGER.debug("Deleted {} published transactional outbox rows", deleted);
      }
    } catch (RuntimeException failure) {
      LOGGER.warn("Transactional outbox cleanup failed; relay polling remains active", failure);
    }
  }

  private static Duration elapsedSince(long started) {
    return Duration.ofNanos(Math.max(0L, System.nanoTime() - started));
  }
}
