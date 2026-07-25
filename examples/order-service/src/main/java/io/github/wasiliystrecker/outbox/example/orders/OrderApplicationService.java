package io.github.wasiliystrecker.outbox.example.orders;

import io.github.wasiliystrecker.outbox.core.OutboxEventMetadata;
import io.github.wasiliystrecker.outbox.spring.TransactionalOutbox;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Clock;
import java.time.Instant;
import java.util.Currency;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
final class OrderApplicationService {
  private static final String CREATED = "CREATED";
  private static final String DESTINATION = "orders.events";

  private final OrderRepository orders;
  private final TransactionalOutbox outbox;
  private final Clock clock;

  OrderApplicationService(OrderRepository orders, TransactionalOutbox outbox, Clock clock) {
    this.orders = orders;
    this.outbox = outbox;
    this.clock = clock;
  }

  @Transactional
  public Order create(CreateOrderCommand command) {
    Objects.requireNonNull(command, "command");
    Instant occurredAt = clock.instant();
    UUID orderId = UUID.randomUUID();
    String currency = Currency.getInstance(command.currency()).getCurrencyCode();
    BigDecimal totalAmount = command.totalAmount().setScale(2, RoundingMode.UNNECESSARY);
    Order order =
        new Order(orderId, command.customerId(), totalAmount, currency, CREATED, 1L, occurredAt);

    orders.insert(order);
    outbox.appendJson(
        new OutboxEventMetadata(
            DESTINATION,
            orderId.toString(),
            "OrderCreated",
            "v1",
            "Order",
            orderId.toString(),
            order.version(),
            occurredAt,
            Map.of("event-source", "order-service")),
        new OrderCreated(
            orderId, order.customerId(), order.totalAmount(), order.currency(), occurredAt));
    return order;
  }

  Order find(UUID id) {
    return orders.findById(id).orElseThrow(() -> new OrderNotFoundException(id));
  }
}
