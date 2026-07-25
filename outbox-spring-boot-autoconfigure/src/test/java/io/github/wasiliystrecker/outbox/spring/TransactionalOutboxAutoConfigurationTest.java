package io.github.wasiliystrecker.outbox.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import io.github.wasiliystrecker.outbox.core.OutboxPublisher;
import io.github.wasiliystrecker.outbox.core.OutboxStore;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

final class TransactionalOutboxAutoConfigurationTest {
  private final ApplicationContextRunner contextRunner =
      new ApplicationContextRunner()
          .withConfiguration(AutoConfigurations.of(TransactionalOutboxAutoConfiguration.class))
          .withBean(DataSource.class, () -> mock(DataSource.class))
          .withBean(ObjectMapper.class, () -> JsonMapper.builder().build());

  @Test
  void configuresTransactionalWritingWhileTheRelayIsDisabled() {
    contextRunner
        .withPropertyValues("outbox.relay.enabled=false")
        .run(
            context -> {
              assertThat(context).hasSingleBean(TransactionalOutbox.class);
              assertThat(context).hasSingleBean(OutboxStore.class);
              assertThat(context).doesNotHaveBean(OutboxRelayLifecycle.class);
            });
  }

  @Test
  void startsTheRelayWithReplaceableStoreAndPublisherBoundaries() {
    contextRunner
        .withPropertyValues(
            "outbox.relay.enabled=true",
            "outbox.relay.poll-interval=1h",
            "outbox.relay.cleanup.enabled=false")
        .withBean(OutboxStore.class, () -> mock(OutboxStore.class))
        .withBean(OutboxPublisher.class, () -> mock(OutboxPublisher.class))
        .run(
            context -> {
              assertThat(context).hasSingleBean(OutboxRelayLifecycle.class);
              assertThat(context.getBean(OutboxRelayLifecycle.class).isRunning()).isTrue();
              assertThat(context).doesNotHaveBean("outboxKafkaProducer");
            });
  }

  @Test
  void rejectsInvalidOperationalBoundsDuringBinding() {
    contextRunner
        .withPropertyValues("outbox.relay.enabled=false", "outbox.relay.batch-size=0")
        .run(
            context -> {
              assertThat(context).hasFailed();
              assertThat(context.getStartupFailure())
                  .hasRootCauseMessage("batchSize must be between 1 and 1000");
            });
  }
}
