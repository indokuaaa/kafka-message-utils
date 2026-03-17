package io.github.indoku.kafka.producer;

import io.github.indoku.kafka.config.KafkaProducerConfig;
import io.github.indoku.kafka.exception.KafkaUtilsException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Simplified Kafka producer that reduces boilerplate for common send operations.
 *
 * <pre>{@code
 * KafkaProducerConfig config = KafkaProducerConfig.builder()
 *     .bootstrapServers("localhost:9092")
 *     .build();
 *
 * try (KafkaMessageProducer<String, String> producer = new KafkaMessageProducer<>(config)) {
 *     // fire-and-forget
 *     producer.send("my-topic", "hello");
 *
 *     // sync (blocks until broker ACK)
 *     SendResult result = producer.sendSync("my-topic", "hello");
 *
 *     // async with callback
 *     producer.sendAsync("my-topic", "hello")
 *             .thenAccept(result -> log.info("Sent: {}", result));
 * }
 * }</pre>
 *
 * @param <K> key type
 * @param <V> value type
 */
public class KafkaMessageProducer<K, V> implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(KafkaMessageProducer.class);

    private final KafkaProducer<K, V> producer;

    public KafkaMessageProducer(KafkaProducerConfig config) {
        this.producer = new KafkaProducer<>(config.toProperties());
    }

    /** Package-private constructor for testing with a mock producer. */
    KafkaMessageProducer(KafkaProducer<K, V> producer) {
        this.producer = producer;
    }

    // -------------------------------------------------------------------------
    // Fire-and-forget
    // -------------------------------------------------------------------------

    /**
     * Sends a message with no key. Does not wait for acknowledgement.
     */
    public void send(String topic, V value) {
        send(topic, null, value);
    }

    /**
     * Sends a message with a key. Does not wait for acknowledgement.
     */
    public void send(String topic, K key, V value) {
        ProducerRecord<K, V> record = new ProducerRecord<>(topic, key, value);
        producer.send(record, (metadata, ex) -> {
            if (ex != null) {
                log.error("Failed to send message to topic={} key={}", topic, key, ex);
            } else {
                log.debug("Sent to topic={} partition={} offset={}", topic, metadata.partition(), metadata.offset());
            }
        });
    }

    // -------------------------------------------------------------------------
    // Synchronous
    // -------------------------------------------------------------------------

    /**
     * Sends and blocks until the broker acknowledges.
     *
     * @throws KafkaUtilsException if sending fails
     */
    public SendResult sendSync(String topic, V value) {
        return sendSync(topic, null, value);
    }

    /**
     * Sends with a key and blocks until the broker acknowledges.
     *
     * @throws KafkaUtilsException if sending fails
     */
    public SendResult sendSync(String topic, K key, V value) {
        ProducerRecord<K, V> record = new ProducerRecord<>(topic, key, value);
        try {
            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata metadata = future.get();
            log.debug("Sent sync to topic={} partition={} offset={}", topic, metadata.partition(), metadata.offset());
            return new SendResult(metadata);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaUtilsException("Interrupted while sending to topic: " + topic, e);
        } catch (ExecutionException e) {
            throw new KafkaUtilsException("Failed to send to topic: " + topic, e.getCause());
        }
    }

    // -------------------------------------------------------------------------
    // Asynchronous (CompletableFuture)
    // -------------------------------------------------------------------------

    /**
     * Sends a message and returns a {@link CompletableFuture} that completes with the send result.
     */
    public CompletableFuture<SendResult> sendAsync(String topic, V value) {
        return sendAsync(topic, null, value);
    }

    /**
     * Sends a message with a key and returns a {@link CompletableFuture} that completes with the send result.
     */
    public CompletableFuture<SendResult> sendAsync(String topic, K key, V value) {
        CompletableFuture<SendResult> completableFuture = new CompletableFuture<>();
        ProducerRecord<K, V> record = new ProducerRecord<>(topic, key, value);
        producer.send(record, (metadata, ex) -> {
            if (ex != null) {
                log.error("Async send failed to topic={} key={}", topic, key, ex);
                completableFuture.completeExceptionally(new KafkaUtilsException(
                        "Async send failed to topic: " + topic, ex));
            } else {
                log.debug("Async sent to topic={} partition={} offset={}", topic, metadata.partition(), metadata.offset());
                completableFuture.complete(new SendResult(metadata));
            }
        });
        return completableFuture;
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    /**
     * Flushes all pending messages.
     */
    public void flush() {
        producer.flush();
    }

    @Override
    public void close() {
        log.info("Closing KafkaMessageProducer");
        producer.close(Duration.ofSeconds(30));
    }
}
