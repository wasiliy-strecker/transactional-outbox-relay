package io.github.wasiliystrecker.outbox.core;

import java.util.Arrays;

/** Immutable payload and metadata supplied when appending an outbox row. */
public final class NewOutboxMessage {
  private final OutboxEventMetadata metadata;
  private final String contentType;
  private final byte[] payload;

  /**
   * Creates a message.
   *
   * @param metadata routing and domain metadata
   * @param contentType payload media type
   * @param payload serialized payload
   */
  public NewOutboxMessage(OutboxEventMetadata metadata, String contentType, byte[] payload) {
    this.metadata = Validation.required(metadata, "metadata");
    this.contentType = Validation.text(contentType, "contentType", 128);
    Validation.required(payload, "payload");
    if (payload.length == 0) {
      throw new IllegalArgumentException("payload must not be empty");
    }
    this.payload = Arrays.copyOf(payload, payload.length);
  }

  /**
   * Returns routing and domain metadata.
   *
   * @return metadata
   */
  public OutboxEventMetadata metadata() {
    return metadata;
  }

  /**
   * Returns the payload media type.
   *
   * @return content type
   */
  public String contentType() {
    return contentType;
  }

  /**
   * Returns a defensive payload copy.
   *
   * @return serialized payload
   */
  public byte[] payload() {
    return Arrays.copyOf(payload, payload.length);
  }

  /**
   * Returns the payload size without copying it.
   *
   * @return bytes
   */
  public int payloadSize() {
    return payload.length;
  }
}
