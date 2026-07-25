package io.github.wasiliystrecker.outbox.core;

import java.util.UUID;

/**
 * Stable identity carried from the database row to the broker message.
 *
 * @param value UUID value
 */
public record OutboxMessageId(UUID value) {
  /** Validates the identity. */
  public OutboxMessageId {
    Validation.required(value, "value");
  }

  /**
   * Creates a random message identity.
   *
   * @return new identity
   */
  public static OutboxMessageId random() {
    return new OutboxMessageId(UUID.randomUUID());
  }

  /**
   * Parses the canonical UUID representation.
   *
   * @param value UUID text
   * @return parsed identity
   */
  public static OutboxMessageId parse(String value) {
    return new OutboxMessageId(UUID.fromString(value));
  }

  @Override
  public String toString() {
    return value.toString();
  }
}
