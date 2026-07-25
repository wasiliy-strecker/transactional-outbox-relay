package io.github.wasiliystrecker.outbox.core;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Atomic persistence boundary used by the relay coordinator.
 *
 * <p>Implementations must support concurrent transition calls from relay worker threads.
 */
public interface OutboxStore {
  /**
   * Claims an available bounded batch.
   *
   * @param request worker and lease bounds
   * @return claimed messages
   */
  List<ClaimedOutboxMessage> claim(ClaimRequest request);

  /**
   * Marks a broker acknowledgement using the current lease token.
   *
   * @param id message identity
   * @param leaseToken current ownership token
   * @param receipt broker acknowledgement
   * @return whether the compare-and-set transition succeeded
   */
  boolean markPublished(OutboxMessageId id, UUID leaseToken, PublishReceipt receipt);

  /**
   * Schedules another publication attempt.
   *
   * @param id message identity
   * @param leaseToken current ownership token
   * @param availableAt next eligible timestamp
   * @param failure sanitized failure
   * @return whether the compare-and-set transition succeeded
   */
  boolean reschedule(
      OutboxMessageId id, UUID leaseToken, Instant availableAt, FailureDetails failure);

  /**
   * Moves a permanently failed message to the dead state.
   *
   * @param id message identity
   * @param leaseToken current ownership token
   * @param failure sanitized failure
   * @return whether the compare-and-set transition succeeded
   */
  boolean markDead(OutboxMessageId id, UUID leaseToken, FailureDetails failure);

  /**
   * Deletes a bounded number of old published rows.
   *
   * @param publishedBefore exclusive acknowledgement cutoff
   * @param limit maximum rows
   * @return deleted rows
   */
  int deletePublishedBefore(Instant publishedBefore, int limit);
}
