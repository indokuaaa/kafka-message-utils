package io.github.indoku.kafka.producer;

import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Result of a successful Kafka send operation.
 */
public class SendResult {

    private final String topic;
    private final int partition;
    private final long offset;
    private final long timestamp;

    public SendResult(RecordMetadata metadata) {
        this.topic = metadata.topic();
        this.partition = metadata.partition();
        this.offset = metadata.offset();
        this.timestamp = metadata.timestamp();
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public long getOffset() {
        return offset;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "SendResult{topic='" + topic + "', partition=" + partition
                + ", offset=" + offset + ", timestamp=" + timestamp + "}";
    }
}
