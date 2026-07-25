package io.github.wasiliystrecker.outbox.core;

/** Durable lifecycle states of an outbox row. */
public enum OutboxStatus {
  /** Available for a relay worker. */
  PENDING,
  /** Exclusively leased by one relay worker. */
  IN_FLIGHT,
  /** Confirmed by the broker and recorded durably. */
  PUBLISHED,
  /** Exhausted retries or failed permanently. */
  DEAD
}
