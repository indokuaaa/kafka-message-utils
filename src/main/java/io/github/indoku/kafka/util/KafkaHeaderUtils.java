package io.github.indoku.kafka.util;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * Utility methods for reading and writing Kafka message headers.
 *
 * <p>Kafka headers are stored as raw {@code byte[]} values. This class removes the boilerplate
 * of charset conversion so callers can work with plain {@link String} values.
 *
 * <pre>{@code
 * // Read
 * String traceId = KafkaHeaderUtils.getString(record, "trace-id").orElse("unknown");
 *
 * // Write
 * ProducerRecord<String, MyEvent> record = new ProducerRecord<>("my-topic", event);
 * KafkaHeaderUtils.setString(record.headers(), "trace-id", traceId);
 * }</pre>
 */
public final class KafkaHeaderUtils {

    private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

    private KafkaHeaderUtils() {
    }

    // -------------------------------------------------------------------------
    // Read
    // -------------------------------------------------------------------------

    /**
     * Returns the last header value for {@code key} decoded as UTF-8, or empty if absent.
     */
    public static Optional<String> getString(Headers headers, String key) {
        return getString(headers, key, DEFAULT_CHARSET);
    }

    /**
     * Returns the last header value for {@code key} decoded with the given {@code charset}, or empty if absent.
     */
    public static Optional<String> getString(Headers headers, String key, Charset charset) {
        Header header = headers.lastHeader(key);
        if (header == null || header.value() == null) {
            return Optional.empty();
        }
        return Optional.of(new String(header.value(), charset));
    }

    /**
     * Convenience overload that reads from a {@link ConsumerRecord}.
     */
    public static Optional<String> getString(ConsumerRecord<?, ?> record, String key) {
        return getString(record.headers(), key);
    }

    /**
     * Returns the last header value for {@code key} as raw bytes, or empty if absent.
     */
    public static Optional<byte[]> getBytes(Headers headers, String key) {
        Header header = headers.lastHeader(key);
        if (header == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(header.value());
    }

    /**
     * Convenience overload that reads bytes from a {@link ConsumerRecord}.
     */
    public static Optional<byte[]> getBytes(ConsumerRecord<?, ?> record, String key) {
        return getBytes(record.headers(), key);
    }

    // -------------------------------------------------------------------------
    // Write
    // -------------------------------------------------------------------------

    /**
     * Adds (or replaces) a header with {@code key} encoded as UTF-8.
     * Any existing headers with the same key are removed first.
     */
    public static void setString(Headers headers, String key, String value) {
        setString(headers, key, value, DEFAULT_CHARSET);
    }

    /**
     * Adds (or replaces) a header with {@code key} encoded with the given {@code charset}.
     * Any existing headers with the same key are removed first.
     */
    public static void setString(Headers headers, String key, String value, Charset charset) {
        headers.remove(key);
        headers.add(new RecordHeader(key, value.getBytes(charset)));
    }

    /**
     * Convenience overload that writes to a {@link ProducerRecord}.
     */
    public static void setString(ProducerRecord<?, ?> record, String key, String value) {
        setString(record.headers(), key, value);
    }

    /**
     * Adds (or replaces) a header with raw byte value.
     * Any existing headers with the same key are removed first.
     */
    public static void setBytes(Headers headers, String key, byte[] value) {
        headers.remove(key);
        headers.add(new RecordHeader(key, value));
    }

    // -------------------------------------------------------------------------
    // Check
    // -------------------------------------------------------------------------

    /**
     * Returns {@code true} if a header with {@code key} exists.
     */
    public static boolean contains(Headers headers, String key) {
        return headers.lastHeader(key) != null;
    }

    /**
     * Convenience overload that checks a {@link ConsumerRecord}.
     */
    public static boolean contains(ConsumerRecord<?, ?> record, String key) {
        return contains(record.headers(), key);
    }
}
