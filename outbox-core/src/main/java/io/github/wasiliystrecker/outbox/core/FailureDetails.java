package io.github.wasiliystrecker.outbox.core;

import java.time.Instant;

/**
 * Sanitized publication failure safe to persist and expose operationally.
 *
 * @param type stable failure category
 * @param message bounded diagnostic message
 * @param failedAt failure timestamp
 */
public record FailureDetails(String type, String message, Instant failedAt) {
  /** Validates bounded diagnostics. */
  public FailureDetails {
    type = Validation.text(type, "type", 200);
    message = Validation.text(message, "message", 1_024);
    failedAt = Validation.required(failedAt, "failedAt");
  }
}
