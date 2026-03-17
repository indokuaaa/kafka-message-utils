package io.github.indoku.kafka.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.Properties;

/**
 * Builder-style configuration for KafkaMessageProducer.
 *
 * <pre>{@code
 * KafkaProducerConfig config = KafkaProducerConfig.builder()
 *     .bootstrapServers("localhost:9092")
 *     .acks("all")
 *     .retries(3)
 *     .build();
 * }</pre>
 */
public class KafkaProducerConfig {

    private final Properties properties;

    private KafkaProducerConfig(Builder builder) {
        this.properties = builder.properties;
    }

    public Properties toProperties() {
        Properties copy = new Properties();
        copy.putAll(properties);
        return copy;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private final Properties properties = new Properties();

        private Builder() {
            // sensible defaults
            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            properties.put(ProducerConfig.ACKS_CONFIG, "all");
            properties.put(ProducerConfig.RETRIES_CONFIG, 3);
            properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        }

        public Builder bootstrapServers(String bootstrapServers) {
            properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            return this;
        }

        public Builder acks(String acks) {
            properties.put(ProducerConfig.ACKS_CONFIG, acks);
            return this;
        }

        public Builder retries(int retries) {
            properties.put(ProducerConfig.RETRIES_CONFIG, retries);
            return this;
        }

        public Builder batchSize(int batchSizeBytes) {
            properties.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSizeBytes);
            return this;
        }

        public Builder lingerMs(int lingerMs) {
            properties.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs);
            return this;
        }

        public Builder bufferMemory(long bufferMemoryBytes) {
            properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferMemoryBytes);
            return this;
        }

        public Builder enableIdempotence(boolean enable) {
            properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, enable);
            return this;
        }

        public Builder compressionType(String compressionType) {
            properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType);
            return this;
        }

        public Builder keySerializer(Class<? extends Serializer<?>> serializerClass) {
            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, serializerClass.getName());
            return this;
        }

        public Builder valueSerializer(Class<? extends Serializer<?>> serializerClass) {
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serializerClass.getName());
            return this;
        }

        public Builder property(String key, Object value) {
            properties.put(key, value);
            return this;
        }

        public Builder properties(Map<String, Object> extraProperties) {
            properties.putAll(extraProperties);
            return this;
        }

        public KafkaProducerConfig build() {
            if (!properties.containsKey(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
                throw new IllegalStateException("bootstrapServers must be specified");
            }
            return new KafkaProducerConfig(this);
        }
    }
}
