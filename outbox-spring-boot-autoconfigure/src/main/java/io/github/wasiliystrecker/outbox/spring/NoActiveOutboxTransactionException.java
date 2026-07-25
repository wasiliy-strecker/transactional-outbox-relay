package io.github.wasiliystrecker.outbox.spring;

/** Raised when an outbox message is appended without an active Spring transaction. */
public final class NoActiveOutboxTransactionException extends IllegalStateException {
  private static final long serialVersionUID = 1L;

  /** Creates the exception with an actionable diagnostic. */
  public NoActiveOutboxTransactionException() {
    super(
        "An active Spring transaction is required so the domain change and outbox message "
            + "commit atomically");
  }
}
