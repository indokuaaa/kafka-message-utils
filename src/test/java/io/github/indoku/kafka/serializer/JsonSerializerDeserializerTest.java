package io.github.indoku.kafka.serializer;

import io.github.indoku.kafka.exception.KafkaUtilsException;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class JsonSerializerDeserializerTest {

    record SampleEvent(String id, String message) {}

    @Test
    void serialize_thenDeserialize_roundTrip() {
        try (JsonSerializer<SampleEvent> serializer = new JsonSerializer<>();
             JsonDeserializer<SampleEvent> deserializer = new JsonDeserializer<>(SampleEvent.class)) {

            SampleEvent original = new SampleEvent("1", "hello");
            byte[] bytes = serializer.serialize("topic", original);
            SampleEvent result = deserializer.deserialize("topic", bytes);

            assertEquals(original.id(), result.id());
            assertEquals(original.message(), result.message());
        }
    }

    @Test
    void serialize_nullValue_returnsNull() {
        try (JsonSerializer<SampleEvent> serializer = new JsonSerializer<>()) {
            assertNull(serializer.serialize("topic", null));
        }
    }

    @Test
    void deserialize_nullBytes_returnsNull() {
        try (JsonDeserializer<SampleEvent> deserializer = new JsonDeserializer<>(SampleEvent.class)) {
            assertNull(deserializer.deserialize("topic", null));
        }
    }

    @Test
    void deserialize_invalidJson_throwsKafkaUtilsException() {
        try (JsonDeserializer<SampleEvent> deserializer = new JsonDeserializer<>(SampleEvent.class)) {
            byte[] invalid = "not-json".getBytes();
            assertThrows(KafkaUtilsException.class, () -> deserializer.deserialize("topic", invalid));
        }
    }

    @Test
    void deserializer_configuresTargetTypeFromProperty() {
        try (JsonSerializer<SampleEvent> serializer = new JsonSerializer<>();
             JsonDeserializer<SampleEvent> deserializer = new JsonDeserializer<>()) {

            deserializer.configure(Map.of(JsonDeserializer.TARGET_TYPE_CONFIG, SampleEvent.class.getName()), false);
            byte[] bytes = serializer.serialize("topic", new SampleEvent("2", "world"));
            SampleEvent result = deserializer.deserialize("topic", bytes);

            assertEquals("2", result.id());
        }
    }

    @Test
    void deserializer_missingTargetType_throwsKafkaUtilsException() {
        try (JsonDeserializer<SampleEvent> deserializer = new JsonDeserializer<>()) {
            assertThrows(KafkaUtilsException.class,
                    () -> deserializer.configure(Map.of(), false));
        }
    }
}
