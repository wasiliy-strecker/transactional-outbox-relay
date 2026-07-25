package io.github.wasiliystrecker.outbox.core;

import java.time.Instant;

/**
 * Broker acknowledgement persisted after a successful send.
 *
 * @param destination acknowledged destination
 * @param partition broker partition
 * @param offset broker offset
 * @param publishedAt acknowledgement timestamp
 */
public record PublishReceipt(String destination, int partition, long offset, Instant publishedAt) {
  /** Validates broker metadata. */
  public PublishReceipt {
    destination = Validation.text(destination, "destination", 249);
    if (partition < 0) {
      throw new IllegalArgumentException("partition must not be negative");
    }
    if (offset < 0) {
      throw new IllegalArgumentException("offset must not be negative");
    }
    publishedAt = Validation.required(publishedAt, "publishedAt");
  }
}
