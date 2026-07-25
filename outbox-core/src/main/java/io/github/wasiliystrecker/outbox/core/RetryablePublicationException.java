package io.github.wasiliystrecker.outbox.core;

/** Publication failure that may succeed on a later attempt. */
public final class RetryablePublicationException extends PublicationException {
  private static final long serialVersionUID = 1L;

  /**
   * Creates a retryable failure.
   *
   * @param message safe diagnostic message
   * @param cause original failure
   */
  public RetryablePublicationException(String message, Throwable cause) {
    super(message, cause);
  }

  @Override
  public boolean retryable() {
    return true;
  }
}
