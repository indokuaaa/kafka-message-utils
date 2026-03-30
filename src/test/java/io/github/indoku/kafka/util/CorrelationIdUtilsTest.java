package io.github.indoku.kafka.util;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class CorrelationIdUtilsTest {

    @Test
    void inject_generatesAndSetsCorrelationId_onProducerRecord() {
        ProducerRecord<String, String> record = new ProducerRecord<>("topic", "value");

        String correlationId = CorrelationIdUtils.inject(record);

        assertNotNull(correlationId);
        assertEquals(correlationId, KafkaHeaderUtils.getString(record.headers(), CorrelationIdUtils.HEADER_KEY).orElse(null));
    }

    @Test
    void inject_withCustomId_overwritesHeader() {
        ProducerRecord<String, String> record = new ProducerRecord<>("topic", "value");
        CorrelationIdUtils.inject(record, "custom-id-001");

        assertEquals("custom-id-001", KafkaHeaderUtils.getString(record.headers(), CorrelationIdUtils.HEADER_KEY).orElse(null));
    }

    @Test
    void inject_onHeaders_generatesAndSetsCorrelationId() {
        Headers headers = new RecordHeaders();

        String correlationId = CorrelationIdUtils.inject(headers);

        assertNotNull(correlationId);
        assertEquals(correlationId, KafkaHeaderUtils.getString(headers, CorrelationIdUtils.HEADER_KEY).orElse(null));
    }

    @Test
    void extract_returnsCorrelationId_fromConsumerRecord() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("topic", 0, 0L, "key", "value");
        CorrelationIdUtils.inject(record.headers());
        String injected = KafkaHeaderUtils.getString(record.headers(), CorrelationIdUtils.HEADER_KEY).orElseThrow();

        Optional<String> extracted = CorrelationIdUtils.extract(record);

        assertTrue(extracted.isPresent());
        assertEquals(injected, extracted.get());
    }

    @Test
    void extract_returnsEmpty_whenNoHeader() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("topic", 0, 0L, "key", "value");

        assertTrue(CorrelationIdUtils.extract(record).isEmpty());
    }

    @Test
    void extractOrGenerate_returnsExisting_whenHeaderPresent() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("topic", 0, 0L, "key", "value");
        CorrelationIdUtils.inject(record.headers());
        String injected = KafkaHeaderUtils.getString(record.headers(), CorrelationIdUtils.HEADER_KEY).orElseThrow();

        assertEquals(injected, CorrelationIdUtils.extractOrGenerate(record));
    }

    @Test
    void extractOrGenerate_generatesNew_whenHeaderAbsent() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("topic", 0, 0L, "key", "value");

        String result = CorrelationIdUtils.extractOrGenerate(record);

        assertNotNull(result);
        assertFalse(result.isBlank());
    }

    @Test
    void generate_returnsUniqueValues() {
        String id1 = CorrelationIdUtils.generate();
        String id2 = CorrelationIdUtils.generate();

        assertNotEquals(id1, id2);
    }
}
