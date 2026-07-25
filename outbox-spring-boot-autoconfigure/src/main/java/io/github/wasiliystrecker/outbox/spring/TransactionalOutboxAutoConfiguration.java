package io.github.wasiliystrecker.outbox.spring;

import io.github.wasiliystrecker.outbox.core.ExponentialBackoffRetryPolicy;
import io.github.wasiliystrecker.outbox.core.OutboxPublisher;
import io.github.wasiliystrecker.outbox.core.OutboxRelay;
import io.github.wasiliystrecker.outbox.core.OutboxStore;
import io.github.wasiliystrecker.outbox.core.RelaySettings;
import io.github.wasiliystrecker.outbox.core.RetryPolicy;
import io.github.wasiliystrecker.outbox.jdbc.JdbcOutboxWriter;
import io.github.wasiliystrecker.outbox.jdbc.PostgresOutboxStore;
import io.github.wasiliystrecker.outbox.kafka.KafkaOutboxPublisher;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import javax.sql.DataSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnSingleCandidate;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import tools.jackson.databind.ObjectMapper;

/** Auto-configures transactional writes and an optional PostgreSQL-to-Kafka relay worker. */
@AutoConfiguration
@ConditionalOnClass({JdbcTemplate.class, KafkaProducer.class})
@ConditionalOnSingleCandidate(DataSource.class)
@EnableConfigurationProperties({OutboxRelayProperties.class, KafkaProperties.class})
public class TransactionalOutboxAutoConfiguration {

  /** Creates the auto-configuration container. */
  public TransactionalOutboxAutoConfiguration() {}

  /**
   * Provides the default UTC clock.
   *
   * @return system UTC clock
   */
  @Bean
  @ConditionalOnMissingBean
  Clock outboxClock() {
    return Clock.systemUTC();
  }

  /**
   * Provides transaction-aware JDBC access dedicated to outbox writes.
   *
   * @param dataSource application data source
   * @return JDBC template
   */
  @Bean("outboxJdbcTemplate")
  @ConditionalOnMissingBean(name = "outboxJdbcTemplate")
  JdbcTemplate outboxJdbcTemplate(DataSource dataSource) {
    return new JdbcTemplate(dataSource);
  }

  /**
   * Provides the low-level JDBC writer.
   *
   * @param objectMapper header serializer
   * @param clock row clock
   * @param properties payload bound
   * @return JDBC writer
   */
  @Bean
  @ConditionalOnMissingBean
  JdbcOutboxWriter jdbcOutboxWriter(
      ObjectMapper objectMapper, Clock clock, OutboxRelayProperties properties) {
    return new JdbcOutboxWriter(objectMapper, clock, properties.maxPayloadBytes());
  }

  /**
   * Provides the application-facing transaction-enforcing facade.
   *
   * @param jdbcTemplate outbox JDBC access
   * @param writer low-level writer
   * @param objectMapper event serializer
   * @return transactional outbox
   */
  @Bean
  @ConditionalOnMissingBean
  TransactionalOutbox transactionalOutbox(
      @Qualifier("outboxJdbcTemplate") JdbcTemplate jdbcTemplate,
      JdbcOutboxWriter writer,
      ObjectMapper objectMapper) {
    return new TransactionalOutbox(jdbcTemplate, writer, objectMapper);
  }

  /**
   * Provides the PostgreSQL claim and transition store.
   *
   * @param dataSource application data source
   * @param objectMapper header deserializer
   * @return PostgreSQL store
   */
  @Bean
  @ConditionalOnMissingBean
  OutboxStore outboxStore(DataSource dataSource, ObjectMapper objectMapper) {
    return new PostgresOutboxStore(dataSource, objectMapper);
  }

  /** Beans that are only required when relay polling is enabled. */
  @Configuration(proxyBeanMethods = false)
  @ConditionalOnProperty(
      prefix = "outbox.relay",
      name = "enabled",
      havingValue = "true",
      matchIfMissing = true)
  static class RelayConfiguration {

    @Bean(name = "outboxKafkaProducer", destroyMethod = "close")
    @ConditionalOnMissingBean(value = OutboxPublisher.class)
    Producer<String, byte[]> outboxKafkaProducer(KafkaProperties kafkaProperties) {
      Map<String, Object> producerProperties =
          new HashMap<>(kafkaProperties.buildProducerProperties());
      producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
      producerProperties.put(
          ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
      producerProperties.putIfAbsent(ProducerConfig.ACKS_CONFIG, "all");
      producerProperties.putIfAbsent(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
      producerProperties.putIfAbsent(ProducerConfig.CLIENT_ID_CONFIG, "outbox-relay");
      return new KafkaProducer<>(producerProperties);
    }

    @Bean
    @ConditionalOnMissingBean
    OutboxPublisher outboxPublisher(
        @Qualifier("outboxKafkaProducer") Producer<String, byte[]> producer,
        OutboxRelayProperties properties,
        Clock clock) {
      return new KafkaOutboxPublisher(producer, properties.acknowledgementTimeout(), clock);
    }

    @Bean
    @ConditionalOnMissingBean
    RetryPolicy outboxRetryPolicy(OutboxRelayProperties properties) {
      OutboxRelayProperties.Retry retry = properties.retry();
      return new ExponentialBackoffRetryPolicy(
          retry.initialDelay(), retry.maximumDelay(), retry.multiplier(), retry.jitterRatio());
    }

    @Bean
    @ConditionalOnMissingBean
    RelaySettings outboxRelaySettings(OutboxRelayProperties properties) {
      return new RelaySettings(
          properties.batchSize(),
          properties.leaseDuration(),
          properties.maxAttempts(),
          properties.maxConcurrency());
    }

    @Bean
    @ConditionalOnMissingBean
    OutboxRelay outboxRelay(
        OutboxStore store,
        OutboxPublisher publisher,
        RetryPolicy retryPolicy,
        RelaySettings settings,
        Clock clock) {
      return new OutboxRelay(store, publisher, retryPolicy, settings, clock);
    }

    @Bean
    @ConditionalOnMissingBean
    RelayMetrics outboxRelayMetrics(ObjectProvider<MeterRegistry> meterRegistries) {
      MeterRegistry registry = meterRegistries.getIfAvailable();
      return registry == null ? new NoopRelayMetrics() : new MicrometerRelayMetrics(registry);
    }

    @Bean
    @ConditionalOnMissingBean
    OutboxRelayLifecycle outboxRelayLifecycle(
        OutboxRelay relay,
        OutboxStore store,
        RelayMetrics metrics,
        OutboxRelayProperties properties,
        Clock clock) {
      return new OutboxRelayLifecycle(
          relay, store, metrics, properties, clock, workerId(properties.workerId()));
    }

    private static String workerId(String configured) {
      if (configured != null && !configured.isBlank()) {
        return configured;
      }
      String host = System.getenv().getOrDefault("HOSTNAME", "localhost");
      return host
          + ":"
          + ProcessHandle.current().pid()
          + ":"
          + UUID.randomUUID().toString().substring(0, 8);
    }
  }
}
