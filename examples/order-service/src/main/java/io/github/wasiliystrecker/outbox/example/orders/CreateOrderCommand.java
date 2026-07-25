package io.github.wasiliystrecker.outbox.example.orders;

import java.math.BigDecimal;

record CreateOrderCommand(String customerId, BigDecimal totalAmount, String currency) {}
