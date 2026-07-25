package io.github.wasiliystrecker.outbox.spring;

/** Raised when a JSON event payload cannot be serialized before persistence. */
public final class OutboxSerializationException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  /**
   * Creates a serialization failure.
   *
   * @param cause Jackson serialization failure
   */
  public OutboxSerializationException(Throwable cause) {
    super("Could not serialize the outbox event payload as JSON", cause);
  }
}
