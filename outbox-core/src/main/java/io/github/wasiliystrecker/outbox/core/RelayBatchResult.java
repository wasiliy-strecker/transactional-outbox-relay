package io.github.wasiliystrecker.outbox.core;

/**
 * Outcome counters for one bounded relay poll.
 *
 * @param claimed messages claimed
 * @param published messages acknowledged and recorded
 * @param rescheduled retryable messages
 * @param dead terminal messages
 * @param ownershipLost compare-and-set transitions rejected
 */
public record RelayBatchResult(
    int claimed, int published, int rescheduled, int dead, int ownershipLost) {
  /** Validates non-negative counters. */
  public RelayBatchResult {
    if (claimed < 0 || published < 0 || rescheduled < 0 || dead < 0 || ownershipLost < 0) {
      throw new IllegalArgumentException("relay counters must not be negative");
    }
    if (published + rescheduled + dead + ownershipLost > claimed) {
      throw new IllegalArgumentException("terminal counters must not exceed claimed messages");
    }
  }
}
