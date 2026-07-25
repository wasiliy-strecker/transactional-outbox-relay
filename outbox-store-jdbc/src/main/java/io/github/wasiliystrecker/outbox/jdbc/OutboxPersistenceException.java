package io.github.wasiliystrecker.outbox.jdbc;

/** Unchecked boundary exception for database and serialization failures. */
public final class OutboxPersistenceException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  /**
   * Creates a persistence failure.
   *
   * @param message safe diagnostic message
   * @param cause original failure
   */
  public OutboxPersistenceException(String message, Throwable cause) {
    super(message, cause);
  }
}
