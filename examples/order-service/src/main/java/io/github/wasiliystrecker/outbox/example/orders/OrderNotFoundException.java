package io.github.wasiliystrecker.outbox.example.orders;

import java.util.UUID;

final class OrderNotFoundException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  OrderNotFoundException(UUID id) {
    super("Order " + id + " was not found");
  }
}
