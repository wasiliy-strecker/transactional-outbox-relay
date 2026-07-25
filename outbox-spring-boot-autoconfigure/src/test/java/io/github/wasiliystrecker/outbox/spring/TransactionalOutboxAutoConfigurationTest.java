package io.github.wasiliystrecker.outbox.spring;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.wasiliystrecker.outbox.core.ClaimRequest;
import io.github.wasiliystrecker.outbox.core.ClaimedOutboxMessage;
import io.github.wasiliystrecker.outbox.core.FailureDetails;
import io.github.wasiliystrecker.outbox.core.OutboxMessageId;
import io.github.wasiliystrecker.outbox.core.OutboxPublisher;
import io.github.wasiliystrecker.outbox.core.OutboxStore;
import io.github.wasiliystrecker.outbox.core.PublishReceipt;
import java.sql.Connection;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.jdbc.datasource.AbstractDataSource;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

final class TransactionalOutboxAutoConfigurationTest {
  private final ApplicationContextRunner contextRunner =
      new ApplicationContextRunner()
          .withConfiguration(AutoConfigurations.of(TransactionalOutboxAutoConfiguration.class))
          .withBean(DataSource.class, TransactionalOutboxAutoConfigurationTest::failingDataSource)
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
        .withBean(OutboxStore.class, EmptyStore::new)
        .withBean(
            OutboxPublisher.class,
            () ->
                message -> {
                  throw new AssertionError("No message should be published");
                })
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

  private static DataSource failingDataSource() {
    return new AbstractDataSource() {
      @Override
      public Connection getConnection() {
        throw new AssertionError("No JDBC connection should be requested");
      }

      @Override
      public Connection getConnection(String username, String password) {
        throw new AssertionError("No JDBC connection should be requested");
      }
    };
  }

  private static final class EmptyStore implements OutboxStore {
    @Override
    public List<ClaimedOutboxMessage> claim(ClaimRequest request) {
      return List.of();
    }

    @Override
    public boolean markPublished(OutboxMessageId id, UUID leaseToken, PublishReceipt receipt) {
      throw new AssertionError("No message should transition");
    }

    @Override
    public boolean reschedule(
        OutboxMessageId id, UUID leaseToken, Instant availableAt, FailureDetails failure) {
      throw new AssertionError("No message should transition");
    }

    @Override
    public boolean markDead(OutboxMessageId id, UUID leaseToken, FailureDetails failure) {
      throw new AssertionError("No message should transition");
    }

    @Override
    public int deletePublishedBefore(Instant publishedBefore, int limit) {
      return 0;
    }
  }
}
