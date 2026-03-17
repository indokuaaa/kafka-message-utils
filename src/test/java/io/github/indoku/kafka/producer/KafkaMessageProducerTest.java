package io.github.indoku.kafka.producer;

import io.github.indoku.kafka.exception.KafkaUtilsException;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class KafkaMessageProducerTest {

    @Mock
    private KafkaProducer<String, String> mockKafkaProducer;

    @Captor
    private ArgumentCaptor<ProducerRecord<String, String>> recordCaptor;

    private KafkaMessageProducer<String, String> producer;

    private static final RecordMetadata METADATA =
            new RecordMetadata(new TopicPartition("test-topic", 0), 0, 42, 1000L, 10, 20);

    @BeforeEach
    void setUp() {
        producer = new KafkaMessageProducer<>(mockKafkaProducer);
    }

    @Test
    void send_noKey_callsProducerWithNullKey() {
        producer.send("test-topic", "hello");

        verify(mockKafkaProducer).send(recordCaptor.capture(), any());

        ProducerRecord<String, String> record = recordCaptor.getValue();
        assertEquals("test-topic", record.topic());
        assertNull(record.key());
        assertEquals("hello", record.value());
    }

    @Test
    void sendSync_returnsCorrectSendResult() throws Exception {
        Future<RecordMetadata> future = CompletableFuture.completedFuture(METADATA);
        when(mockKafkaProducer.send(any())).thenReturn(future);

        SendResult result = producer.sendSync("test-topic", "hello");

        assertEquals("test-topic", result.getTopic());
        assertEquals(0, result.getPartition());
        assertEquals(42L, result.getOffset());
    }

    @Test
    void sendSync_whenInterrupted_throwsKafkaUtilsException() throws Exception {
        @SuppressWarnings("unchecked")
        Future<RecordMetadata> future = mock(Future.class);
        when(mockKafkaProducer.send(any())).thenReturn(future);
        when(future.get()).thenThrow(new InterruptedException("interrupted"));

        assertThrows(KafkaUtilsException.class, () -> producer.sendSync("test-topic", "hello"));
        assertTrue(Thread.interrupted()); // restore and verify flag was set
    }

    @Test
    void sendAsync_completesWithSendResult() throws Exception {
        doAnswer(invocation -> {
            Callback cb = invocation.getArgument(1);
            cb.onCompletion(METADATA, null);
            return null;
        }).when(mockKafkaProducer).send(any(), any());

        SendResult result = producer.sendAsync("test-topic", "hello").get();

        assertEquals("test-topic", result.getTopic());
        assertEquals(42L, result.getOffset());
    }

    @Test
    void sendAsync_completesExceptionallyOnError() {
        doAnswer(invocation -> {
            Callback cb = invocation.getArgument(1);
            cb.onCompletion(null, new RuntimeException("broker down"));
            return null;
        }).when(mockKafkaProducer).send(any(), any());

        CompletableFuture<SendResult> future = producer.sendAsync("test-topic", "hello");

        assertTrue(future.isCompletedExceptionally());
    }

    @Test
    void close_callsProducerClose() {
        producer.close();
        verify(mockKafkaProducer).close(any());
    }
}
