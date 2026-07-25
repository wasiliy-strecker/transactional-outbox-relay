package io.github.wasiliystrecker.outbox.jdbc;

import io.github.wasiliystrecker.outbox.core.ClaimRequest;
import io.github.wasiliystrecker.outbox.core.ClaimedOutboxMessage;
import io.github.wasiliystrecker.outbox.core.FailureDetails;
import io.github.wasiliystrecker.outbox.core.OutboxEventMetadata;
import io.github.wasiliystrecker.outbox.core.OutboxMessageId;
import io.github.wasiliystrecker.outbox.core.OutboxStore;
import io.github.wasiliystrecker.outbox.core.PublishReceipt;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.sql.DataSource;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

/** PostgreSQL implementation using short transactions, row locks, leases, and CAS transitions. */
public final class PostgresOutboxStore implements OutboxStore {
  private static final TypeReference<Map<String, String>> HEADERS_TYPE = new TypeReference<>() {};

  private static final String CLAIM =
      """
      WITH candidates AS (
          SELECT id
          FROM outbox_message
          WHERE (status = 'PENDING' AND available_at <= ?)
             OR (status = 'IN_FLIGHT' AND lease_until <= ?)
          ORDER BY available_at, created_at, id
          FOR UPDATE SKIP LOCKED
          LIMIT ?
      )
      UPDATE outbox_message AS message
      SET status = 'IN_FLIGHT',
          attempt_count = message.attempt_count + 1,
          lease_owner = ?,
          lease_token = gen_random_uuid(),
          lease_until = ?,
          updated_at = ?
      FROM candidates
      WHERE message.id = candidates.id
      RETURNING message.id, message.destination, message.partition_key,
          message.event_type, message.schema_version, message.aggregate_type,
          message.aggregate_id, message.aggregate_version, message.occurred_at,
          message.content_type, message.headers::text AS headers_json,
          message.payload, message.attempt_count, message.lease_token,
          message.lease_until, message.created_at
      """;

  private final DataSource dataSource;
  private final ObjectMapper objectMapper;

  /**
   * Creates the PostgreSQL store.
   *
   * @param dataSource database connections
   * @param objectMapper header deserializer
   */
  public PostgresOutboxStore(DataSource dataSource, ObjectMapper objectMapper) {
    this.dataSource = java.util.Objects.requireNonNull(dataSource, "dataSource");
    this.objectMapper = java.util.Objects.requireNonNull(objectMapper, "objectMapper");
  }

  @Override
  public List<ClaimedOutboxMessage> claim(ClaimRequest request) {
    java.util.Objects.requireNonNull(request, "request");
    return transaction(
        connection -> {
          try (PreparedStatement statement = connection.prepareStatement(CLAIM)) {
            statement.setTimestamp(1, Timestamp.from(request.now()));
            statement.setTimestamp(2, Timestamp.from(request.now()));
            statement.setInt(3, request.batchSize());
            statement.setString(4, request.workerId());
            statement.setTimestamp(5, Timestamp.from(request.leaseUntil()));
            statement.setTimestamp(6, Timestamp.from(request.now()));
            try (ResultSet rows = statement.executeQuery()) {
              List<ClaimedOutboxMessage> messages = new ArrayList<>();
              while (rows.next()) {
                messages.add(readClaim(rows));
              }
              return List.copyOf(messages);
            }
          }
        });
  }

  @Override
  public boolean markPublished(OutboxMessageId id, UUID leaseToken, PublishReceipt receipt) {
    java.util.Objects.requireNonNull(receipt, "receipt");
    String sql =
        """
        UPDATE outbox_message
        SET status = 'PUBLISHED', published_at = ?, broker_partition = ?,
            broker_offset = ?, lease_owner = NULL, lease_token = NULL,
            lease_until = NULL, updated_at = ?
        WHERE id = ? AND status = 'IN_FLIGHT' AND lease_token = ?
          AND lease_until >= ?
        """;
    return transition(
        sql,
        statement -> {
          statement.setTimestamp(1, Timestamp.from(receipt.publishedAt()));
          statement.setInt(2, receipt.partition());
          statement.setLong(3, receipt.offset());
          statement.setTimestamp(4, Timestamp.from(receipt.publishedAt()));
          statement.setObject(5, id.value());
          statement.setObject(6, leaseToken);
          statement.setTimestamp(7, Timestamp.from(receipt.publishedAt()));
        });
  }

  @Override
  public boolean reschedule(
      OutboxMessageId id, UUID leaseToken, Instant availableAt, FailureDetails failure) {
    String sql =
        """
        UPDATE outbox_message
        SET status = 'PENDING', available_at = ?, lease_owner = NULL,
            lease_token = NULL, lease_until = NULL, last_error_type = ?,
            last_error_message = ?, last_error_at = ?, updated_at = ?
        WHERE id = ? AND status = 'IN_FLIGHT' AND lease_token = ?
          AND lease_until >= ?
        """;
    return transition(
        sql,
        statement -> {
          statement.setTimestamp(1, Timestamp.from(availableAt));
          setFailure(statement, 2, failure);
          statement.setTimestamp(5, Timestamp.from(failure.failedAt()));
          statement.setObject(6, id.value());
          statement.setObject(7, leaseToken);
          statement.setTimestamp(8, Timestamp.from(failure.failedAt()));
        });
  }

  @Override
  public boolean markDead(OutboxMessageId id, UUID leaseToken, FailureDetails failure) {
    String sql =
        """
        UPDATE outbox_message
        SET status = 'DEAD', lease_owner = NULL, lease_token = NULL,
            lease_until = NULL, last_error_type = ?, last_error_message = ?,
            last_error_at = ?, updated_at = ?
        WHERE id = ? AND status = 'IN_FLIGHT' AND lease_token = ?
          AND lease_until >= ?
        """;
    return transition(
        sql,
        statement -> {
          setFailure(statement, 1, failure);
          statement.setTimestamp(4, Timestamp.from(failure.failedAt()));
          statement.setObject(5, id.value());
          statement.setObject(6, leaseToken);
          statement.setTimestamp(7, Timestamp.from(failure.failedAt()));
        });
  }

  @Override
  public int deletePublishedBefore(Instant publishedBefore, int limit) {
    java.util.Objects.requireNonNull(publishedBefore, "publishedBefore");
    if (limit < 1 || limit > 10_000) {
      throw new IllegalArgumentException("limit must be between 1 and 10000");
    }
    String sql =
        """
        DELETE FROM outbox_message
        WHERE id IN (
            SELECT id FROM outbox_message
            WHERE status = 'PUBLISHED' AND published_at < ?
            ORDER BY published_at, id
            LIMIT ?
        )
        """;
    return transaction(
        connection -> {
          try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setTimestamp(1, Timestamp.from(publishedBefore));
            statement.setInt(2, limit);
            return statement.executeUpdate();
          }
        });
  }

  private boolean transition(String sql, StatementBinder binder) {
    return transaction(
        connection -> {
          try (PreparedStatement statement = connection.prepareStatement(sql)) {
            binder.bind(statement);
            return statement.executeUpdate() == 1;
          }
        });
  }

  private ClaimedOutboxMessage readClaim(ResultSet row) throws SQLException {
    try {
      OutboxEventMetadata metadata =
          new OutboxEventMetadata(
              row.getString("destination"),
              row.getString("partition_key"),
              row.getString("event_type"),
              row.getString("schema_version"),
              row.getString("aggregate_type"),
              row.getString("aggregate_id"),
              row.getLong("aggregate_version"),
              row.getTimestamp("occurred_at").toInstant(),
              objectMapper.readValue(row.getString("headers_json"), HEADERS_TYPE));
      return new ClaimedOutboxMessage(
          new OutboxMessageId(row.getObject("id", UUID.class)),
          metadata,
          row.getString("content_type"),
          row.getBytes("payload"),
          row.getInt("attempt_count"),
          row.getObject("lease_token", UUID.class),
          row.getTimestamp("lease_until").toInstant(),
          row.getTimestamp("created_at").toInstant());
    } catch (RuntimeException failure) {
      throw new OutboxPersistenceException("Could not reconstruct claimed message", failure);
    }
  }

  private static void setFailure(
      PreparedStatement statement, int firstIndex, FailureDetails failure) throws SQLException {
    java.util.Objects.requireNonNull(failure, "failure");
    statement.setString(firstIndex, failure.type());
    statement.setString(firstIndex + 1, failure.message());
    statement.setTimestamp(firstIndex + 2, Timestamp.from(failure.failedAt()));
  }

  private <T> T transaction(SqlWork<T> work) {
    try (Connection connection = dataSource.getConnection()) {
      connection.setAutoCommit(false);
      try {
        T result = work.execute(connection);
        connection.commit();
        return result;
      } catch (SQLException | RuntimeException failure) {
        rollback(connection, failure);
        throw failure;
      }
    } catch (SQLException failure) {
      throw new OutboxPersistenceException("PostgreSQL outbox operation failed", failure);
    }
  }

  private static void rollback(Connection connection, Throwable original) {
    try {
      connection.rollback();
    } catch (SQLException rollbackFailure) {
      original.addSuppressed(rollbackFailure);
    }
  }

  @FunctionalInterface
  private interface SqlWork<T> {
    T execute(Connection connection) throws SQLException;
  }

  @FunctionalInterface
  private interface StatementBinder {
    void bind(PreparedStatement statement) throws SQLException;
  }
}
