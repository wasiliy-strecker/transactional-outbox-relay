package io.github.wasiliystrecker.outbox.example.orders;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
class OrderRepository {
  private final JdbcTemplate jdbcTemplate;

  OrderRepository(JdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  void insert(Order order) {
    jdbcTemplate.update(
        """
        INSERT INTO purchase_order (
            id, customer_id, total_amount, currency, status, version, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        order.id(),
        order.customerId(),
        order.totalAmount(),
        order.currency(),
        order.status(),
        order.version(),
        Timestamp.from(order.createdAt()));
  }

  Optional<Order> findById(UUID id) {
    return jdbcTemplate
        .query(
            """
            SELECT id, customer_id, total_amount, currency, status, version, created_at
            FROM purchase_order
            WHERE id = ?
            """,
            OrderRepository::read,
            id)
        .stream()
        .findFirst();
  }

  private static Order read(ResultSet row, int rowNumber) throws SQLException {
    return new Order(
        row.getObject("id", UUID.class),
        row.getString("customer_id"),
        row.getBigDecimal("total_amount"),
        row.getString("currency"),
        row.getString("status"),
        row.getLong("version"),
        row.getTimestamp("created_at").toInstant());
  }
}
