package io.github.indoku.kafka.util;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.*;

class MessageTimestampUtilsTest {

    // 2024-01-15T12:30:00Z
    private static final long EPOCH_MS = 1705321800000L;

    private ConsumerRecord<String, String> createRecord() {
        return new ConsumerRecord<>("topic", 0, 0L, EPOCH_MS,
                TimestampType.CREATE_TIME, 0, 0, "key", "value", new RecordHeaders(), null);
    }

    @Test
    void toInstant_fromRecord() {
        Instant instant = MessageTimestampUtils.toInstant(createRecord());

        assertEquals(Instant.ofEpochMilli(EPOCH_MS), instant);
    }

    @Test
    void toInstant_fromLong() {
        Instant instant = MessageTimestampUtils.toInstant(EPOCH_MS);

        assertEquals(EPOCH_MS, instant.toEpochMilli());
    }

    @Test
    void toLocalDateTime_fromRecord_withZone() {
        LocalDateTime ldt = MessageTimestampUtils.toLocalDateTime(createRecord(), ZoneOffset.UTC);

        assertEquals(2024, ldt.getYear());
        assertEquals(1, ldt.getMonthValue());
        assertEquals(15, ldt.getDayOfMonth());
        assertEquals(12, ldt.getHour());
        assertEquals(30, ldt.getMinute());
    }

    @Test
    void toLocalDateTime_fromLong_withZone() {
        LocalDateTime ldt = MessageTimestampUtils.toLocalDateTime(EPOCH_MS, ZoneOffset.UTC);

        assertEquals(LocalDateTime.of(2024, 1, 15, 12, 30, 0), ldt);
    }

    @Test
    void toLocalDateTime_fromRecord_usesSystemDefault() {
        LocalDateTime ldt = MessageTimestampUtils.toLocalDateTime(createRecord());
        LocalDateTime expected = LocalDateTime.ofInstant(Instant.ofEpochMilli(EPOCH_MS), ZoneId.systemDefault());

        assertEquals(expected, ldt);
    }
}
