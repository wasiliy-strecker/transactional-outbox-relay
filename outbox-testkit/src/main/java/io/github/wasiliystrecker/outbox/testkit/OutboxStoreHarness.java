package io.github.wasiliystrecker.outbox.testkit;

import io.github.wasiliystrecker.outbox.core.NewOutboxMessage;
import io.github.wasiliystrecker.outbox.core.OutboxMessageId;
import io.github.wasiliystrecker.outbox.core.OutboxStatus;
import java.time.Instant;

/** Persistence-specific operations required by the reusable store contract. */
public interface OutboxStoreHarness {
  /** Removes state left by a previous contract test. */
  void reset();

  /**
   * Appends and commits one message.
   *
   * @param message message to append
   * @param createdAt deterministic row timestamp
   * @return generated identity
   */
  OutboxMessageId append(NewOutboxMessage message, Instant createdAt);

  /**
   * Reads the durable state of one row.
   *
   * @param id message identity
   * @return current state
   */
  OutboxStatus status(OutboxMessageId id);
}
