package io.github.indoku.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Functional interface for processing a consumed Kafka record.
 *
 * @param <K> key type
 * @param <V> value type
 */
@FunctionalInterface
public interface MessageHandler<K, V> {

    /**
     * Handle a single Kafka record.
     *
     * @param record the consumed record
     * @throws Exception any processing error (will be caught and logged by the consumer loop)
     */
    void handle(ConsumerRecord<K, V> record) throws Exception;
}
