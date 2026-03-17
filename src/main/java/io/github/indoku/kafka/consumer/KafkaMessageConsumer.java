package io.github.indoku.kafka.consumer;

import io.github.indoku.kafka.config.KafkaConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Simplified Kafka consumer that manages the poll loop and offset commits.
 *
 * <pre>{@code
 * KafkaConsumerConfig config = KafkaConsumerConfig.builder()
 *     .bootstrapServers("localhost:9092")
 *     .groupId("my-group")
 *     .build();
 *
 * KafkaMessageConsumer<String, String> consumer = new KafkaMessageConsumer<>(config);
 *
 * // starts polling in a background thread
 * consumer.subscribe("my-topic", record -> {
 *     System.out.println("Received: " + record.value());
 * });
 *
 * // graceful shutdown
 * consumer.close();
 * }</pre>
 *
 * @param <K> key type
 * @param <V> value type
 */
public class KafkaMessageConsumer<K, V> implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(KafkaMessageConsumer.class);

    private final KafkaConsumer<K, V> consumer;
    private final Duration pollTimeout;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private ExecutorService executor;

    public KafkaMessageConsumer(KafkaConsumerConfig config) {
        this(config, Duration.ofMillis(500));
    }

    public KafkaMessageConsumer(KafkaConsumerConfig config, Duration pollTimeout) {
        this.consumer = new KafkaConsumer<>(config.toProperties());
        this.pollTimeout = pollTimeout;
    }

    /** Package-private constructor for testing with a mock consumer. */
    KafkaMessageConsumer(KafkaConsumer<K, V> consumer, Duration pollTimeout) {
        this.consumer = consumer;
        this.pollTimeout = pollTimeout;
    }

    // -------------------------------------------------------------------------
    // Subscribe
    // -------------------------------------------------------------------------

    /**
     * Subscribes to one or more topics and starts polling in the calling thread.
     * Blocks until {@link #close()} is called.
     */
    public void subscribeAndPoll(MessageHandler<K, V> handler, String... topics) {
        subscribeAndPoll(handler, Arrays.asList(topics));
    }

    /**
     * Subscribes to a collection of topics and starts polling in the calling thread.
     * Blocks until {@link #close()} is called.
     */
    public void subscribeAndPoll(MessageHandler<K, V> handler, Collection<String> topics) {
        consumer.subscribe(topics);
        log.info("Subscribed to topics: {}", topics);
        running.set(true);
        pollLoop(handler);
    }

    /**
     * Subscribes to one or more topics and starts polling in a background thread.
     * Returns immediately.
     */
    public void subscribe(MessageHandler<K, V> handler, String... topics) {
        subscribe(handler, Arrays.asList(topics));
    }

    /**
     * Subscribes to a collection of topics and starts polling in a background thread.
     * Returns immediately.
     */
    public void subscribe(MessageHandler<K, V> handler, Collection<String> topics) {
        consumer.subscribe(topics);
        log.info("Subscribed to topics in background: {}", topics);
        running.set(true);
        executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "kafka-consumer-thread");
            t.setDaemon(true);
            return t;
        });
        executor.submit(() -> pollLoop(handler));
    }

    // -------------------------------------------------------------------------
    // Poll loop
    // -------------------------------------------------------------------------

    private void pollLoop(MessageHandler<K, V> handler) {
        try {
            while (running.get()) {
                ConsumerRecords<K, V> records = consumer.poll(pollTimeout);
                if (records.isEmpty()) {
                    continue;
                }

                Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

                for (ConsumerRecord<K, V> record : records) {
                    try {
                        handler.handle(record);
                        offsets.put(
                                new TopicPartition(record.topic(), record.partition()),
                                new OffsetAndMetadata(record.offset() + 1)
                        );
                    } catch (Exception e) {
                        log.error("Error handling record from topic={} partition={} offset={}",
                                record.topic(), record.partition(), record.offset(), e);
                    }
                }

                if (!offsets.isEmpty()) {
                    consumer.commitSync(offsets);
                }
            }
        } catch (WakeupException e) {
            if (running.get()) {
                throw e;
            }
            // expected on close()
        } finally {
            consumer.close();
            log.info("Consumer closed");
        }
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    @Override
    public void close() {
        log.info("Stopping KafkaMessageConsumer");
        running.set(false);
        consumer.wakeup();

        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    public boolean isRunning() {
        return running.get();
    }
}
