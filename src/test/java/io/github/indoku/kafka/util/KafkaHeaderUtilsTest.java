package io.github.indoku.kafka.util;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.*;

class KafkaHeaderUtilsTest {

    private Headers headers;

    @BeforeEach
    void setUp() {
        headers = new RecordHeaders();
    }

    // -------------------------------------------------------------------------
    // getString
    // -------------------------------------------------------------------------

    @Test
    void getString_returnsValue_whenHeaderExists() {
        KafkaHeaderUtils.setString(headers, "trace-id", "abc-123");

        assertTrue(KafkaHeaderUtils.getString(headers, "trace-id").isPresent());
        assertEquals("abc-123", KafkaHeaderUtils.getString(headers, "trace-id").get());
    }

    @Test
    void getString_returnsEmpty_whenHeaderAbsent() {
        assertTrue(KafkaHeaderUtils.getString(headers, "missing").isEmpty());
    }

    @Test
    void getString_fromConsumerRecord_works() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("topic", 0, 0L, "key", "value");
        record.headers().add("env", "prod".getBytes(StandardCharsets.UTF_8));

        assertEquals("prod", KafkaHeaderUtils.getString(record, "env").orElse(null));
    }

    // -------------------------------------------------------------------------
    // setString / replace
    // -------------------------------------------------------------------------

    @Test
    void setString_replacesExistingHeader() {
        KafkaHeaderUtils.setString(headers, "trace-id", "first");
        KafkaHeaderUtils.setString(headers, "trace-id", "second");

        assertEquals("second", KafkaHeaderUtils.getString(headers, "trace-id").get());

        long count = StreamSupport
                .stream(headers.headers("trace-id").spliterator(), false)
                .count();
        assertEquals(1, count);
    }

    @Test
    void setString_onProducerRecord_works() {
        ProducerRecord<String, String> record = new ProducerRecord<>("topic", "value");
        KafkaHeaderUtils.setString(record, "source", "order-service");

        assertEquals("order-service", KafkaHeaderUtils.getString(record.headers(), "source").orElse(null));
    }

    // -------------------------------------------------------------------------
    // getBytes / setBytes
    // -------------------------------------------------------------------------

    @Test
    void getBytes_returnsRawBytes_whenHeaderExists() {
        byte[] raw = new byte[]{1, 2, 3};
        KafkaHeaderUtils.setBytes(headers, "bin", raw);

        assertArrayEquals(raw, KafkaHeaderUtils.getBytes(headers, "bin").orElse(null));
    }

    @Test
    void getBytes_returnsEmpty_whenHeaderAbsent() {
        assertTrue(KafkaHeaderUtils.getBytes(headers, "bin").isEmpty());
    }

    // -------------------------------------------------------------------------
    // contains
    // -------------------------------------------------------------------------

    @Test
    void contains_returnsTrue_whenHeaderExists() {
        KafkaHeaderUtils.setString(headers, "x-request-id", "req-1");

        assertTrue(KafkaHeaderUtils.contains(headers, "x-request-id"));
    }

    @Test
    void contains_returnsFalse_whenHeaderAbsent() {
        assertFalse(KafkaHeaderUtils.contains(headers, "x-request-id"));
    }

    @Test
    void contains_fromConsumerRecord_works() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("topic", 0, 0L, null, null);
        record.headers().add("flag", new byte[]{1});

        assertTrue(KafkaHeaderUtils.contains(record, "flag"));
        assertFalse(KafkaHeaderUtils.contains(record, "other"));
    }
}
