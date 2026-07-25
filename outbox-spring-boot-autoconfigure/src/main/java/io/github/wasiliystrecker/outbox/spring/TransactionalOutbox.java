package io.github.wasiliystrecker.outbox.spring;

import io.github.wasiliystrecker.outbox.core.NewOutboxMessage;
import io.github.wasiliystrecker.outbox.core.OutboxEventMetadata;
import io.github.wasiliystrecker.outbox.core.OutboxMessageId;
import io.github.wasiliystrecker.outbox.jdbc.JdbcOutboxWriter;
import java.util.Objects;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

/**
 * Application-facing writer that joins the current Spring-managed JDBC transaction.
 *
 * <p>Call this API from a {@code @Transactional} application service after changing domain state.
 * The outbox insert then commits or rolls back with that state change.
 */
public final class TransactionalOutbox {
  private static final String JSON_CONTENT_TYPE = "application/json";

  private final JdbcTemplate jdbcTemplate;
  private final JdbcOutboxWriter writer;
  private final ObjectMapper objectMapper;

  /**
   * Creates the transactional facade.
   *
   * @param jdbcTemplate transaction-aware JDBC access
   * @param writer low-level outbox writer
   * @param objectMapper event serializer
   */
  public TransactionalOutbox(
      JdbcTemplate jdbcTemplate, JdbcOutboxWriter writer, ObjectMapper objectMapper) {
    this.jdbcTemplate = Objects.requireNonNull(jdbcTemplate, "jdbcTemplate must not be null");
    this.writer = Objects.requireNonNull(writer, "writer must not be null");
    this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
  }

  /**
   * Serializes and appends a JSON event in the current transaction.
   *
   * @param metadata routing and domain metadata
   * @param event event payload
   * @return durable message identity
   */
  public OutboxMessageId appendJson(OutboxEventMetadata metadata, Object event) {
    Objects.requireNonNull(metadata, "metadata must not be null");
    Objects.requireNonNull(event, "event must not be null");
    requireTransaction();
    try {
      return appendSerialized(
          new NewOutboxMessage(metadata, JSON_CONTENT_TYPE, objectMapper.writeValueAsBytes(event)));
    } catch (JacksonException exception) {
      throw new OutboxSerializationException(exception);
    }
  }

  /**
   * Appends an already serialized event in the current transaction.
   *
   * @param message serialized event
   * @return durable message identity
   */
  public OutboxMessageId append(NewOutboxMessage message) {
    Objects.requireNonNull(message, "message must not be null");
    requireTransaction();
    return appendSerialized(message);
  }

  private OutboxMessageId appendSerialized(NewOutboxMessage message) {
    ConnectionCallback<OutboxMessageId> append = connection -> writer.append(connection, message);
    return jdbcTemplate.execute(append);
  }

  private static void requireTransaction() {
    if (!TransactionSynchronizationManager.isActualTransactionActive()) {
      throw new NoActiveOutboxTransactionException();
    }
  }
}
