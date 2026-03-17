package io.github.indoku.kafka.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.indoku.kafka.exception.KafkaUtilsException;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Kafka {@link Serializer} that converts any object to JSON bytes using Jackson.
 *
 * <pre>{@code
 * KafkaProducerConfig config = KafkaProducerConfig.builder()
 *     .bootstrapServers("localhost:9092")
 *     .valueSerializer(JsonSerializer.class)
 *     .build();
 * }</pre>
 *
 * @param <T> the type to serialize
 */
public class JsonSerializer<T> implements Serializer<T> {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, T data) {
        if (data == null) {
            return null;
        }
        try {
            return OBJECT_MAPPER.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new KafkaUtilsException("Failed to serialize object to JSON for topic: " + topic, e);
        }
    }
}
