package io.github.wasiliystrecker.outbox.spring;

import io.github.wasiliystrecker.outbox.core.RelayBatchResult;
import java.time.Duration;

interface RelayMetrics {
  void recordBatch(RelayBatchResult result, Duration duration);

  void recordFailure(Duration duration);

  void recordDeleted(int count);
}
