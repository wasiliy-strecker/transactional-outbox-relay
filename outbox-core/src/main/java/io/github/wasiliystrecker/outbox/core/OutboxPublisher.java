package io.github.wasiliystrecker.outbox.core;

/** Broker boundary used by the relay coordinator. */
@FunctionalInterface
public interface OutboxPublisher {
  /**
   * Publishes one claimed message and waits for broker acknowledgement.
   *
   * @param message current claimed message
   * @return broker receipt
   * @throws PublicationException when publication cannot be confirmed
   */
  PublishReceipt publish(ClaimedOutboxMessage message) throws PublicationException;
}
