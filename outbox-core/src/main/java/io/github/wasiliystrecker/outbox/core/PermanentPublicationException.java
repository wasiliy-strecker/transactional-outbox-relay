package io.github.wasiliystrecker.outbox.core;

/** Publication failure that cannot succeed without changing configuration or data. */
public final class PermanentPublicationException extends PublicationException {
  private static final long serialVersionUID = 1L;

  /**
   * Creates a permanent failure.
   *
   * @param message safe diagnostic message
   * @param cause original failure
   */
  public PermanentPublicationException(String message, Throwable cause) {
    super(message, cause);
  }

  @Override
  public boolean retryable() {
    return false;
  }
}
