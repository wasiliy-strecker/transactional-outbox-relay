package io.github.wasiliystrecker.outbox.example.orders;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

record OrderCreated(
    UUID orderId, String customerId, BigDecimal totalAmount, String currency, Instant occurredAt) {}
