package io.github.wasiliystrecker.outbox.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

import java.time.Duration;
import org.junit.jupiter.api.Test;

final class ExponentialBackoffRetryPolicyTest {
  @Test
  void growsExponentiallyAndCapsBeforeApplyingPositiveJitter() {
    OutboxMessageId id = OutboxMessageId.random();
    RetryPolicy policy =
        new ExponentialBackoffRetryPolicy(
            Duration.ofSeconds(1), Duration.ofSeconds(10), 2.0, 0.25, (messageId, attempt) -> 1.0);

    assertThat(policy.delay(id, 1)).isEqualTo(Duration.ofMillis(1_250));
    assertThat(policy.delay(id, 2)).isEqualTo(Duration.ofMillis(2_500));
    assertThat(policy.delay(id, 8)).isEqualTo(Duration.ofSeconds(10));
  }

  @Test
  void rejectsAnInvalidJitterSample() {
    RetryPolicy policy =
        new ExponentialBackoffRetryPolicy(
            Duration.ofSeconds(1), Duration.ofSeconds(10), 2.0, 0.25, (messageId, attempt) -> 1.1);

    assertThatIllegalStateException()
        .isThrownBy(() -> policy.delay(OutboxMessageId.random(), 1))
        .withMessageContaining("jitterSource");
  }
}
