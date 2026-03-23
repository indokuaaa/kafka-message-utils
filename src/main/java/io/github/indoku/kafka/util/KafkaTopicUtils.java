package io.github.indoku.kafka.util;

import io.github.indoku.kafka.exception.KafkaUtilsException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Utility methods for Kafka topic management via {@link AdminClient}.
 *
 * <pre>{@code
 * try (KafkaTopicUtils utils = new KafkaTopicUtils("localhost:9092")) {
 *     boolean exists = utils.exists("my-topic");
 *     int partitions = utils.partitionCount("my-topic");
 *     utils.createIfAbsent("new-topic", 3, (short) 1);
 * }
 * }</pre>
 */
public class KafkaTopicUtils implements AutoCloseable {

    private final AdminClient adminClient;

    public KafkaTopicUtils(String bootstrapServers) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        this.adminClient = AdminClient.create(props);
    }

    public KafkaTopicUtils(AdminClient adminClient) {
        this.adminClient = adminClient;
    }

    // -------------------------------------------------------------------------
    // 조회
    // -------------------------------------------------------------------------

    /**
     * 토픽이 존재하는지 확인합니다.
     */
    public boolean exists(String topic) {
        try {
            Set<String> topics = adminClient.listTopics().names().get();
            return topics.contains(topic);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaUtilsException("Interrupted while checking topic existence: " + topic, e);
        } catch (ExecutionException e) {
            throw new KafkaUtilsException("Failed to check topic existence: " + topic, e);
        }
    }

    /**
     * 토픽의 파티션 수를 반환합니다.
     *
     * @throws KafkaUtilsException 토픽이 존재하지 않거나 조회 실패 시
     */
    public int partitionCount(String topic) {
        try {
            Map<String, TopicDescription> descriptions =
                    adminClient.describeTopics(Collections.singleton(topic)).allTopicNames().get();
            return descriptions.get(topic).partitions().size();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaUtilsException("Interrupted while getting partition count: " + topic, e);
        } catch (ExecutionException e) {
            throw new KafkaUtilsException("Failed to get partition count: " + topic, e);
        }
    }

    /**
     * 모든 토픽 이름 목록을 반환합니다.
     */
    public Set<String> listTopics() {
        try {
            return adminClient.listTopics().names().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaUtilsException("Interrupted while listing topics", e);
        } catch (ExecutionException e) {
            throw new KafkaUtilsException("Failed to list topics", e);
        }
    }

    // -------------------------------------------------------------------------
    // 생성 / 삭제
    // -------------------------------------------------------------------------

    /**
     * 토픽이 없을 때만 생성합니다. 이미 존재하면 아무 작업도 하지 않습니다.
     *
     * @param topic              토픽명
     * @param numPartitions      파티션 수
     * @param replicationFactor  복제 인수
     * @return 실제로 생성됐으면 {@code true}, 이미 존재했으면 {@code false}
     */
    public boolean createIfAbsent(String topic, int numPartitions, short replicationFactor) {
        if (exists(topic)) {
            return false;
        }
        try {
            NewTopic newTopic = new NewTopic(topic, numPartitions, replicationFactor);
            adminClient.createTopics(Collections.singleton(newTopic)).all().get();
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaUtilsException("Interrupted while creating topic: " + topic, e);
        } catch (ExecutionException e) {
            throw new KafkaUtilsException("Failed to create topic: " + topic, e);
        }
    }

    /**
     * 토픽을 삭제합니다.
     *
     * @throws KafkaUtilsException 삭제 실패 시
     */
    public void delete(String topic) {
        try {
            adminClient.deleteTopics(Collections.singleton(topic)).all().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaUtilsException("Interrupted while deleting topic: " + topic, e);
        } catch (ExecutionException e) {
            throw new KafkaUtilsException("Failed to delete topic: " + topic, e);
        }
    }

    @Override
    public void close() {
        adminClient.close();
    }
}
