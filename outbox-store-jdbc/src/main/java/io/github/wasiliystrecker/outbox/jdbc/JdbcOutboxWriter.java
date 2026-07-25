package io.github.wasiliystrecker.outbox.jdbc;

import io.github.wasiliystrecker.outbox.core.NewOutboxMessage;
import io.github.wasiliystrecker.outbox.core.OutboxMessageId;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import org.postgresql.util.PGobject;
import tools.jackson.databind.ObjectMapper;

/** Appends an outbox row through a caller-owned JDBC transaction. */
public final class JdbcOutboxWriter {
  private static final String INSERT =
      """
      INSERT INTO outbox_message (
          id, destination, partition_key, event_type, schema_version,
          aggregate_type, aggregate_id, aggregate_version, occurred_at,
          content_type, headers, payload, status, attempt_count,
          available_at, created_at, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', 0, ?, ?, ?)
      """;

  private final ObjectMapper objectMapper;
  private final Clock clock;
  private final int maxPayloadBytes;

  /**
   * Creates a writer.
   *
   * @param objectMapper header serializer
   * @param clock row timestamp source
   * @param maxPayloadBytes maximum serialized payload size
   */
  public JdbcOutboxWriter(ObjectMapper objectMapper, Clock clock, int maxPayloadBytes) {
    this.objectMapper = java.util.Objects.requireNonNull(objectMapper, "objectMapper");
    this.clock = java.util.Objects.requireNonNull(clock, "clock");
    if (maxPayloadBytes < 1) {
      throw new IllegalArgumentException("maxPayloadBytes must be positive");
    }
    this.maxPayloadBytes = maxPayloadBytes;
  }

  /**
   * Appends a message without committing or closing the supplied connection.
   *
   * @param connection caller-owned transaction connection
   * @param message serialized message
   * @return generated identity
   */
  public OutboxMessageId append(Connection connection, NewOutboxMessage message) {
    return append(connection, OutboxMessageId.random(), message, clock.instant());
  }

  /**
   * Appends a message with deterministic identity and time.
   *
   * @param connection caller-owned transaction connection
   * @param id message identity
   * @param message serialized message
   * @param createdAt row timestamp
   * @return supplied identity
   */
  public OutboxMessageId append(
      Connection connection, OutboxMessageId id, NewOutboxMessage message, Instant createdAt) {
    java.util.Objects.requireNonNull(connection, "connection");
    java.util.Objects.requireNonNull(id, "id");
    java.util.Objects.requireNonNull(message, "message");
    java.util.Objects.requireNonNull(createdAt, "createdAt");
    if (message.payloadSize() > maxPayloadBytes) {
      throw new IllegalArgumentException(
          "payload exceeds configured limit of " + maxPayloadBytes + " bytes");
    }
    try (PreparedStatement statement = connection.prepareStatement(INSERT)) {
      var metadata = message.metadata();
      statement.setObject(1, id.value());
      statement.setString(2, metadata.destination());
      statement.setString(3, metadata.partitionKey());
      statement.setString(4, metadata.eventType());
      statement.setString(5, metadata.schemaVersion());
      statement.setString(6, metadata.aggregateType());
      statement.setString(7, metadata.aggregateId());
      statement.setLong(8, metadata.aggregateVersion());
      statement.setTimestamp(9, Timestamp.from(metadata.occurredAt()));
      statement.setString(10, message.contentType());
      statement.setObject(11, jsonb(objectMapper.writeValueAsString(metadata.headers())));
      statement.setBytes(12, message.payload());
      statement.setTimestamp(13, Timestamp.from(createdAt));
      statement.setTimestamp(14, Timestamp.from(createdAt));
      statement.setTimestamp(15, Timestamp.from(createdAt));
      statement.executeUpdate();
      return id;
    } catch (SQLException | RuntimeException failure) {
      throw new OutboxPersistenceException("Could not append outbox message", failure);
    }
  }

  private static PGobject jsonb(String value) throws SQLException {
    PGobject json = new PGobject();
    json.setType("jsonb");
    json.setValue(value);
    return json;
  }
}
