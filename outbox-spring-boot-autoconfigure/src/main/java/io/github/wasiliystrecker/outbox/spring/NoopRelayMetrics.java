package io.github.wasiliystrecker.outbox.spring;

import io.github.wasiliystrecker.outbox.core.RelayBatchResult;
import java.time.Duration;

final class NoopRelayMetrics implements RelayMetrics {
  @Override
  public void recordBatch(RelayBatchResult result, Duration duration) {}

  @Override
  public void recordFailure(Duration duration) {}

  @Override
  public void recordDeleted(int count) {}
}
