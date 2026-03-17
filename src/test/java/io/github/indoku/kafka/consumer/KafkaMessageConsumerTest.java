package io.github.indoku.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class KafkaMessageConsumerTest {

    @Mock
    private KafkaConsumer<String, String> mockKafkaConsumer;

    private KafkaMessageConsumer<String, String> consumer;

    @BeforeEach
    void setUp() {
        consumer = new KafkaMessageConsumer<>(mockKafkaConsumer, Duration.ofMillis(100));
    }

    @Test
    void subscribe_startsBackgroundThread() throws InterruptedException {
        TopicPartition partition = new TopicPartition("test-topic", 0);
        ConsumerRecord<String, String> record = new ConsumerRecord<>("test-topic", 0, 0L, "key", "value");
        ConsumerRecords<String, String> records = new ConsumerRecords<>(Map.of(partition, List.of(record)));

        when(mockKafkaConsumer.poll(any(Duration.class)))
                .thenReturn(records)
                .thenThrow(new WakeupException());

        List<String> received = new ArrayList<>();
        consumer.subscribe(r -> received.add(r.value()), "test-topic");

        Thread.sleep(300);
        consumer.close();

        assertEquals(1, received.size());
        assertEquals("value", received.get(0));
    }

    @Test
    void subscribeAndPoll_handlerExceptionDoesNotStopLoop() throws Exception {
        TopicPartition partition = new TopicPartition("test-topic", 0);
        ConsumerRecord<String, String> record1 = new ConsumerRecord<>("test-topic", 0, 0L, null, "first");
        ConsumerRecord<String, String> record2 = new ConsumerRecord<>("test-topic", 0, 1L, null, "second");

        ConsumerRecords<String, String> batch = new ConsumerRecords<>(
                Map.of(partition, List.of(record1, record2)));

        when(mockKafkaConsumer.poll(any(Duration.class)))
                .thenReturn(batch)
                .thenThrow(new WakeupException());

        List<String> received = new ArrayList<>();
        MessageHandler<String, String> failingHandler = r -> {
            if ("first".equals(r.value())) throw new RuntimeException("intentional error");
            received.add(r.value());
        };

        // run in a separate thread since subscribeAndPoll blocks
        Thread t = new Thread(() -> consumer.subscribeAndPoll(failingHandler, "test-topic"));
        t.start();
        t.join(2000);

        // second record should still be processed despite first failing
        assertEquals(1, received.size());
        assertEquals("second", received.get(0));
    }

    @Test
    void close_setsRunningToFalse() {
        consumer.close();
        assertFalse(consumer.isRunning());
    }

    @Test
    void close_callsWakeup() {
        consumer.close();
        verify(mockKafkaConsumer).wakeup();
    }
}
