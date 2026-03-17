package io.github.indoku.kafka.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.indoku.kafka.exception.KafkaUtilsException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * Kafka {@link Deserializer} that converts JSON bytes to a target type using Jackson.
 *
 * <p>The target class must be passed either via the constructor or via the
 * {@code "json.deserializer.target.type"} consumer property.
 *
 * <pre>{@code
 * // Option 1: register via constructor (programmatic config)
 * JsonDeserializer<OrderEvent> deserializer = new JsonDeserializer<>(OrderEvent.class);
 *
 * // Option 2: register via consumer property key
 * KafkaConsumerConfig config = KafkaConsumerConfig.builder()
 *     .bootstrapServers("localhost:9092")
 *     .groupId("my-group")
 *     .valueDeserializer(JsonDeserializer.class)
 *     .property(JsonDeserializer.TARGET_TYPE_CONFIG, OrderEvent.class.getName())
 *     .build();
 * }</pre>
 *
 * @param <T> the type to deserialize into
 */
public class JsonDeserializer<T> implements Deserializer<T> {

    public static final String TARGET_TYPE_CONFIG = "json.deserializer.target.type";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private Class<T> targetType;

    /** No-arg constructor required by Kafka (use with {@value #TARGET_TYPE_CONFIG} property). */
    public JsonDeserializer() {
    }

    public JsonDeserializer(Class<T> targetType) {
        this.targetType = targetType;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void configure(Map<String, ?> configs, boolean isKey) {
        if (targetType == null) {
            String className = (String) configs.get(TARGET_TYPE_CONFIG);
            if (className == null) {
                throw new KafkaUtilsException(
                        "JsonDeserializer requires either a constructor argument or the '"
                                + TARGET_TYPE_CONFIG + "' property");
            }
            try {
                targetType = (Class<T>) Class.forName(className);
            } catch (ClassNotFoundException e) {
                throw new KafkaUtilsException("Cannot find target class: " + className, e);
            }
        }
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            return OBJECT_MAPPER.readValue(data, targetType);
        } catch (Exception e) {
            throw new KafkaUtilsException("Failed to deserialize JSON from topic: " + topic, e);
        }
    }
}
