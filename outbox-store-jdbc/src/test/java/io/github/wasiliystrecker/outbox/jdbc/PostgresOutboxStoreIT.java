package io.github.wasiliystrecker.outbox.jdbc;

import io.github.wasiliystrecker.outbox.core.NewOutboxMessage;
import io.github.wasiliystrecker.outbox.core.OutboxMessageId;
import io.github.wasiliystrecker.outbox.core.OutboxStatus;
import io.github.wasiliystrecker.outbox.core.OutboxStore;
import io.github.wasiliystrecker.outbox.testkit.OutboxStoreContract;
import io.github.wasiliystrecker.outbox.testkit.OutboxStoreHarness;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.ObjectMapper;

@Testcontainers
final class PostgresOutboxStoreIT extends OutboxStoreContract {
  @Container
  private static final PostgreSQLContainer POSTGRES =
      new PostgreSQLContainer("postgres:18.3-alpine");

  private static DataSource dataSource;
  private static PostgresOutboxStore store;
  private static JdbcOutboxWriter writer;
  private static OutboxStoreHarness harness;

  @BeforeAll
  static void prepareDatabase() {
    PGSimpleDataSource configured = new PGSimpleDataSource();
    configured.setURL(POSTGRES.getJdbcUrl());
    configured.setUser(POSTGRES.getUsername());
    configured.setPassword(POSTGRES.getPassword());
    dataSource = configured;
    Flyway.configure()
        .dataSource(dataSource)
        .locations("classpath:db/outbox/postgresql")
        .load()
        .migrate();
    ObjectMapper objectMapper = new ObjectMapper();
    store = new PostgresOutboxStore(dataSource, objectMapper);
    writer =
        new JdbcOutboxWriter(
            objectMapper,
            Clock.fixed(Instant.parse("2026-07-25T08:00:00Z"), ZoneOffset.UTC),
            1_048_576);
    harness = new PostgresHarness();
  }

  @Override
  protected OutboxStore store() {
    return store;
  }

  @Override
  protected OutboxStoreHarness harness() {
    return harness;
  }

  private static final class PostgresHarness implements OutboxStoreHarness {
    @Override
    public void reset() {
      execute("TRUNCATE TABLE outbox_message");
    }

    @Override
    public OutboxMessageId append(NewOutboxMessage message, Instant createdAt) {
      try (Connection connection = dataSource.getConnection()) {
        connection.setAutoCommit(false);
        OutboxMessageId id = OutboxMessageId.random();
        writer.append(connection, id, message, createdAt);
        connection.commit();
        return id;
      } catch (SQLException failure) {
        throw new OutboxPersistenceException("Could not append contract fixture", failure);
      }
    }

    @Override
    public OutboxStatus status(OutboxMessageId id) {
      try (Connection connection = dataSource.getConnection();
          PreparedStatement statement =
              connection.prepareStatement("SELECT status FROM outbox_message WHERE id = ?")) {
        statement.setObject(1, id.value());
        try (ResultSet result = statement.executeQuery()) {
          return result.next() ? OutboxStatus.valueOf(result.getString(1)) : null;
        }
      } catch (SQLException failure) {
        throw new OutboxPersistenceException("Could not read contract state", failure);
      }
    }

    private static void execute(String sql) {
      try (Connection connection = dataSource.getConnection();
          PreparedStatement statement = connection.prepareStatement(sql)) {
        statement.executeUpdate();
      } catch (SQLException failure) {
        throw new OutboxPersistenceException("Could not reset contract state", failure);
      }
    }
  }
}
