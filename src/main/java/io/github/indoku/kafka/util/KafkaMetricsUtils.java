package io.github.indoku.kafka.util;

import io.github.indoku.kafka.exception.KafkaUtilsException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * Kafka 메트릭 조회 유틸리티.
 *
 * <p>Consumer group lag, 커밋된 오프셋, 최신(end) 오프셋 등을 간단하게 조회합니다.
 *
 * <pre>{@code
 * try (KafkaMetricsUtils metrics = new KafkaMetricsUtils("localhost:9092")) {
 *     // 그룹 전체 lag
 *     long totalLag = metrics.totalLag("my-group");
 *
 *     // 파티션별 lag
 *     Map<TopicPartition, Long> lagMap = metrics.lagPerPartition("my-group");
 *
 *     // 커밋된 오프셋 조회
 *     Map<TopicPartition, Long> offsets = metrics.committedOffsets("my-group");
 * }
 * }</pre>
 */
public class KafkaMetricsUtils implements AutoCloseable {

    private final AdminClient adminClient;

    public KafkaMetricsUtils(String bootstrapServers) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        this.adminClient = AdminClient.create(props);
    }

    public KafkaMetricsUtils(AdminClient adminClient) {
        this.adminClient = adminClient;
    }

    // -------------------------------------------------------------------------
    // Consumer Lag
    // -------------------------------------------------------------------------

    /**
     * 컨슈머 그룹의 파티션별 lag를 반환합니다.
     *
     * <p>lag = endOffset - committedOffset
     *
     * @param groupId 컨슈머 그룹 ID
     * @return 파티션별 lag (커밋된 오프셋이 없는 파티션은 제외)
     */
    public Map<TopicPartition, Long> lagPerPartition(String groupId) {
        Map<TopicPartition, Long> committed = committedOffsets(groupId);
        if (committed.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<TopicPartition, Long> endOffsets = endOffsets(committed.keySet());

        Map<TopicPartition, Long> lagMap = new LinkedHashMap<>();
        for (Map.Entry<TopicPartition, Long> entry : committed.entrySet()) {
            TopicPartition tp = entry.getKey();
            long commitOffset = entry.getValue();
            long endOffset = endOffsets.getOrDefault(tp, commitOffset);
            lagMap.put(tp, Math.max(0, endOffset - commitOffset));
        }
        return lagMap;
    }

    /**
     * 컨슈머 그룹의 특정 토픽에 대한 파티션별 lag를 반환합니다.
     */
    public Map<TopicPartition, Long> lagPerPartition(String groupId, String topic) {
        return lagPerPartition(groupId).entrySet().stream()
                .filter(e -> e.getKey().topic().equals(topic))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (a, b) -> a, LinkedHashMap::new));
    }

    /**
     * 컨슈머 그룹의 전체 lag 합계를 반환합니다.
     */
    public long totalLag(String groupId) {
        return lagPerPartition(groupId).values().stream()
                .mapToLong(Long::longValue)
                .sum();
    }

    /**
     * 컨슈머 그룹의 특정 토픽에 대한 전체 lag 합계를 반환합니다.
     */
    public long totalLag(String groupId, String topic) {
        return lagPerPartition(groupId, topic).values().stream()
                .mapToLong(Long::longValue)
                .sum();
    }

    // -------------------------------------------------------------------------
    // Offsets
    // -------------------------------------------------------------------------

    /**
     * 컨슈머 그룹의 커밋된 오프셋을 파티션별로 반환합니다.
     */
    public Map<TopicPartition, Long> committedOffsets(String groupId) {
        try {
            ListConsumerGroupOffsetsResult result = adminClient.listConsumerGroupOffsets(groupId);
            Map<TopicPartition, OffsetAndMetadata> offsets = result.partitionsToOffsetAndMetadata().get();
            Map<TopicPartition, Long> map = new LinkedHashMap<>();
            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
                if (entry.getValue() != null) {
                    map.put(entry.getKey(), entry.getValue().offset());
                }
            }
            return map;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaUtilsException("Interrupted while fetching committed offsets for group: " + groupId, e);
        } catch (ExecutionException e) {
            throw new KafkaUtilsException("Failed to fetch committed offsets for group: " + groupId, e);
        }
    }

    /**
     * 지정된 파티션들의 end offset(최신 오프셋)을 반환합니다.
     */
    public Map<TopicPartition, Long> endOffsets(Set<TopicPartition> partitions) {
        try (org.apache.kafka.clients.consumer.KafkaConsumer<byte[], byte[]> consumer =
                     createMetricsConsumer()) {
            return consumer.endOffsets(partitions).entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (a, b) -> a, LinkedHashMap::new));
        }
    }

    // -------------------------------------------------------------------------
    // Consumer Groups
    // -------------------------------------------------------------------------

    /**
     * 브로커에 등록된 모든 컨슈머 그룹 ID 목록을 반환합니다.
     */
    public Set<String> listConsumerGroups() {
        try {
            return adminClient.listConsumerGroups().all().get().stream()
                    .map(g -> g.groupId())
                    .collect(Collectors.toCollection(TreeSet::new));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaUtilsException("Interrupted while listing consumer groups", e);
        } catch (ExecutionException e) {
            throw new KafkaUtilsException("Failed to list consumer groups", e);
        }
    }

    // -------------------------------------------------------------------------
    // Internal
    // -------------------------------------------------------------------------

    private org.apache.kafka.clients.consumer.KafkaConsumer<byte[], byte[]> createMetricsConsumer() {
        Map<String, Object> config = adminClient.metrics().values().stream()
                .findFirst()
                .map(m -> m.metricName().tags())
                .map(tags -> tags.get("client-id"))
                .map(clientId -> {
                    Map<String, Object> props = new HashMap<>();
                    props.put(org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                            getBootstrapServers());
                    props.put(org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                            org.apache.kafka.common.serialization.ByteArrayDeserializer.class.getName());
                    props.put(org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                            org.apache.kafka.common.serialization.ByteArrayDeserializer.class.getName());
                    return props;
                })
                .orElseGet(() -> {
                    Map<String, Object> props = new HashMap<>();
                    props.put(org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                            getBootstrapServers());
                    props.put(org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                            org.apache.kafka.common.serialization.ByteArrayDeserializer.class.getName());
                    props.put(org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                            org.apache.kafka.common.serialization.ByteArrayDeserializer.class.getName());
                    return props;
                });
        return new org.apache.kafka.clients.consumer.KafkaConsumer<>(config);
    }

    private String getBootstrapServers() {
        // AdminClient 내부 메트릭에서 bootstrap.servers를 추출하기 어려우므로
        // describeCluster를 통해 브로커 주소를 가져옴
        try {
            return adminClient.describeCluster().nodes().get().stream()
                    .map(node -> node.host() + ":" + node.port())
                    .collect(Collectors.joining(","));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaUtilsException("Interrupted while resolving bootstrap servers", e);
        } catch (ExecutionException e) {
            throw new KafkaUtilsException("Failed to resolve bootstrap servers", e);
        }
    }

    @Override
    public void close() {
        adminClient.close();
    }
}
