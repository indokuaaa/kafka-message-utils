package io.github.indoku.kafka.util;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

/**
 * Kafka 레코드 타임스탬프를 Java 시간 타입으로 변환하는 유틸리티.
 *
 * <pre>{@code
 * Instant instant = MessageTimestampUtils.toInstant(record);
 * LocalDateTime ldt = MessageTimestampUtils.toLocalDateTime(record);
 * LocalDateTime utc = MessageTimestampUtils.toLocalDateTime(record, ZoneId.of("UTC"));
 * }</pre>
 */
public final class MessageTimestampUtils {

    private MessageTimestampUtils() {
    }

    public static Instant toInstant(ConsumerRecord<?, ?> record) {
        return Instant.ofEpochMilli(record.timestamp());
    }

    public static Instant toInstant(long timestampMs) {
        return Instant.ofEpochMilli(timestampMs);
    }

    public static LocalDateTime toLocalDateTime(ConsumerRecord<?, ?> record) {
        return toLocalDateTime(record, ZoneId.systemDefault());
    }

    public static LocalDateTime toLocalDateTime(ConsumerRecord<?, ?> record, ZoneId zoneId) {
        return LocalDateTime.ofInstant(toInstant(record), zoneId);
    }

    public static LocalDateTime toLocalDateTime(long timestampMs) {
        return toLocalDateTime(timestampMs, ZoneId.systemDefault());
    }

    public static LocalDateTime toLocalDateTime(long timestampMs, ZoneId zoneId) {
        return LocalDateTime.ofInstant(toInstant(timestampMs), zoneId);
    }
}
