package io.github.wasiliystrecker.outbox.core;

import java.time.Instant;
import java.util.Arrays;
import java.util.UUID;

/** Immutable message leased to exactly one current worker token. */
public final class ClaimedOutboxMessage {
  private final OutboxMessageId id;
  private final OutboxEventMetadata metadata;
  private final String contentType;
  private final byte[] payload;
  private final int attempt;
  private final UUID leaseToken;
  private final Instant leaseUntil;
  private final Instant createdAt;

  /**
   * Creates a claimed message.
   *
   * @param id durable message identity
   * @param metadata routing and domain metadata
   * @param contentType payload media type
   * @param payload serialized payload
   * @param attempt one-based publication attempt
   * @param leaseToken compare-and-set ownership token
   * @param leaseUntil ownership deadline
   * @param createdAt row creation timestamp
   */
  public ClaimedOutboxMessage(
      OutboxMessageId id,
      OutboxEventMetadata metadata,
      String contentType,
      byte[] payload,
      int attempt,
      UUID leaseToken,
      Instant leaseUntil,
      Instant createdAt) {
    this.id = Validation.required(id, "id");
    this.metadata = Validation.required(metadata, "metadata");
    this.contentType = Validation.text(contentType, "contentType", 128);
    Validation.required(payload, "payload");
    if (payload.length == 0) {
      throw new IllegalArgumentException("payload must not be empty");
    }
    this.payload = Arrays.copyOf(payload, payload.length);
    if (attempt < 1) {
      throw new IllegalArgumentException("attempt must be positive");
    }
    this.attempt = attempt;
    this.leaseToken = Validation.required(leaseToken, "leaseToken");
    this.leaseUntil = Validation.required(leaseUntil, "leaseUntil");
    this.createdAt = Validation.required(createdAt, "createdAt");
  }

  /**
   * Returns the durable identity.
   *
   * @return durable identity
   */
  public OutboxMessageId id() {
    return id;
  }

  /**
   * Returns routing and domain metadata.
   *
   * @return routing and domain metadata
   */
  public OutboxEventMetadata metadata() {
    return metadata;
  }

  /**
   * Returns the payload media type.
   *
   * @return payload media type
   */
  public String contentType() {
    return contentType;
  }

  /**
   * Returns a defensive payload copy.
   *
   * @return defensive payload copy
   */
  public byte[] payload() {
    return Arrays.copyOf(payload, payload.length);
  }

  /**
   * Returns the one-based publication attempt.
   *
   * @return one-based attempt
   */
  public int attempt() {
    return attempt;
  }

  /**
   * Returns the compare-and-set ownership token.
   *
   * @return compare-and-set ownership token
   */
  public UUID leaseToken() {
    return leaseToken;
  }

  /**
   * Returns the ownership deadline.
   *
   * @return ownership deadline
   */
  public Instant leaseUntil() {
    return leaseUntil;
  }

  /**
   * Returns the row creation timestamp.
   *
   * @return row creation timestamp
   */
  public Instant createdAt() {
    return createdAt;
  }
}
