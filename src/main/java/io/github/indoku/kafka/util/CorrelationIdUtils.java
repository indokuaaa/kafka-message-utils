package io.github.indoku.kafka.util;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;

import java.util.Optional;
import java.util.UUID;

/**
 * Utility methods for managing correlation-id headers in Kafka messages.
 *
 * <p>correlation-id는 요청 추적(tracing)을 위해 메시지에 부여하는 고유 식별자입니다.
 * Producer에서 생성하고, Consumer에서 추출하여 로그에 남기는 패턴으로 주로 사용됩니다.
 *
 * <pre>{@code
 * // Producer: correlation-id 생성 후 헤더에 주입
 * ProducerRecord<String, String> record = new ProducerRecord<>("my-topic", "value");
 * String correlationId = CorrelationIdUtils.inject(record);
 *
 * // Consumer: 헤더에서 correlation-id 추출
 * String correlationId = CorrelationIdUtils.extract(record)
 *         .orElse("unknown");
 * }</pre>
 */
public final class CorrelationIdUtils {

    public static final String HEADER_KEY = "correlation-id";

    private CorrelationIdUtils() {
    }

    // -------------------------------------------------------------------------
    // Inject (Producer 쪽)
    // -------------------------------------------------------------------------

    /**
     * UUID 기반 correlation-id를 생성하여 {@link ProducerRecord} 헤더에 주입합니다.
     *
     * @return 생성된 correlation-id
     */
    public static String inject(ProducerRecord<?, ?> record) {
        String correlationId = generate();
        KafkaHeaderUtils.setString(record.headers(), HEADER_KEY, correlationId);
        return correlationId;
    }

    /**
     * 지정한 correlation-id를 {@link ProducerRecord} 헤더에 주입합니다.
     * 기존 값이 있으면 덮어씁니다.
     */
    public static void inject(ProducerRecord<?, ?> record, String correlationId) {
        KafkaHeaderUtils.setString(record.headers(), HEADER_KEY, correlationId);
    }

    /**
     * UUID 기반 correlation-id를 생성하여 {@link Headers}에 주입합니다.
     *
     * @return 생성된 correlation-id
     */
    public static String inject(Headers headers) {
        String correlationId = generate();
        KafkaHeaderUtils.setString(headers, HEADER_KEY, correlationId);
        return correlationId;
    }

    // -------------------------------------------------------------------------
    // Extract (Consumer 쪽)
    // -------------------------------------------------------------------------

    /**
     * {@link ConsumerRecord} 헤더에서 correlation-id를 추출합니다.
     *
     * @return correlation-id, 헤더가 없으면 {@link Optional#empty()}
     */
    public static Optional<String> extract(ConsumerRecord<?, ?> record) {
        return KafkaHeaderUtils.getString(record.headers(), HEADER_KEY);
    }

    /**
     * {@link Headers}에서 correlation-id를 추출합니다.
     */
    public static Optional<String> extract(Headers headers) {
        return KafkaHeaderUtils.getString(headers, HEADER_KEY);
    }

    /**
     * correlation-id가 없으면 새로 생성해서 반환합니다. (헤더에 주입하지 않음)
     */
    public static String extractOrGenerate(ConsumerRecord<?, ?> record) {
        return extract(record).orElseGet(CorrelationIdUtils::generate);
    }

    // -------------------------------------------------------------------------
    // Etc
    // -------------------------------------------------------------------------

    /**
     * UUID v4 기반 correlation-id를 생성합니다.
     */
    public static String generate() {
        return UUID.randomUUID().toString();
    }
}
