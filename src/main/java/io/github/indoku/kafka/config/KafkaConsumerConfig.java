package io.github.indoku.kafka.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Map;
import java.util.Properties;

/**
 * Builder-style configuration for KafkaMessageConsumer.
 *
 * <pre>{@code
 * KafkaConsumerConfig config = KafkaConsumerConfig.builder()
 *     .bootstrapServers("localhost:9092")
 *     .groupId("my-group")
 *     .autoOffsetReset("earliest")
 *     .build();
 * }</pre>
 */
public class KafkaConsumerConfig {

    private final Properties properties;

    private KafkaConsumerConfig(Builder builder) {
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
            properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        }

        public Builder bootstrapServers(String bootstrapServers) {
            properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            return this;
        }

        public Builder groupId(String groupId) {
            properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            return this;
        }

        public Builder autoOffsetReset(String autoOffsetReset) {
            properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
            return this;
        }

        public Builder enableAutoCommit(boolean enable) {
            properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enable);
            return this;
        }

        public Builder autoCommitIntervalMs(int intervalMs) {
            properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, intervalMs);
            return this;
        }

        public Builder maxPollRecords(int maxPollRecords) {
            properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
            return this;
        }

        public Builder sessionTimeoutMs(int timeoutMs) {
            properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, timeoutMs);
            return this;
        }

        public Builder heartbeatIntervalMs(int intervalMs) {
            properties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, intervalMs);
            return this;
        }

        public Builder keyDeserializer(Class<? extends Deserializer<?>> deserializerClass) {
            properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializerClass.getName());
            return this;
        }

        public Builder valueDeserializer(Class<? extends Deserializer<?>> deserializerClass) {
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializerClass.getName());
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

        public KafkaConsumerConfig build() {
            if (!properties.containsKey(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
                throw new IllegalStateException("bootstrapServers must be specified");
            }
            if (!properties.containsKey(ConsumerConfig.GROUP_ID_CONFIG)) {
                throw new IllegalStateException("groupId must be specified");
            }
            return new KafkaConsumerConfig(this);
        }
    }
}
