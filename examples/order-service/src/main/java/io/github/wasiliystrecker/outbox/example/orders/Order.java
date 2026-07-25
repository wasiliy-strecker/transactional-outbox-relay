package io.github.wasiliystrecker.outbox.example.orders;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

record Order(
    UUID id,
    String customerId,
    BigDecimal totalAmount,
    String currency,
    String status,
    long version,
    Instant createdAt) {}
