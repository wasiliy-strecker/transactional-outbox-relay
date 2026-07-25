package io.github.wasiliystrecker.outbox.core;

/** Base checked exception for classified broker publication failures. */
public abstract class PublicationException extends Exception {
  private static final long serialVersionUID = 1L;

  /**
   * Creates a classified exception.
   *
   * @param message safe diagnostic message
   * @param cause original failure
   */
  protected PublicationException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Indicates whether another attempt may succeed without changing the message.
   *
   * @return retry classification
   */
  public abstract boolean retryable();
}
