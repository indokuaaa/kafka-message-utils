package io.github.indoku.kafka.util;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.Node;

import java.time.Duration;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Kafka Broker 연결 상태를 확인하는 HealthChecker.
 *
 * <pre>{@code
 * try (KafkaHealthChecker checker = new KafkaHealthChecker("localhost:9092")) {
 *     boolean healthy = checker.isHealthy();
 *     int brokerCount = checker.brokerCount();
 *     String clusterId = checker.clusterId();
 * }
 * }</pre>
 */
public class KafkaHealthChecker implements AutoCloseable {

    private static final long DEFAULT_TIMEOUT_MS = 5000L;

    private final AdminClient adminClient;
    private final long timeoutMs;

    public KafkaHealthChecker(String bootstrapServers) {
        this(bootstrapServers, DEFAULT_TIMEOUT_MS);
    }

    public KafkaHealthChecker(String bootstrapServers, long timeoutMs) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(timeoutMs));
        this.adminClient = AdminClient.create(props);
        this.timeoutMs = timeoutMs;
    }

    public KafkaHealthChecker(AdminClient adminClient) {
        this(adminClient, DEFAULT_TIMEOUT_MS);
    }

    public KafkaHealthChecker(AdminClient adminClient, long timeoutMs) {
        this.adminClient = adminClient;
        this.timeoutMs = timeoutMs;
    }

    /**
     * Broker에 연결 가능하면 {@code true}, 아니면 {@code false}.
     */
    public boolean isHealthy() {
        try {
            adminClient.describeCluster().nodes().get(timeoutMs, TimeUnit.MILLISECONDS);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 현재 활성 브로커 수를 반환합니다.
     *
     * @throws KafkaHealthCheckException 연결 실패 시
     */
    public int brokerCount() {
        try {
            Collection<Node> nodes = adminClient.describeCluster()
                    .nodes().get(timeoutMs, TimeUnit.MILLISECONDS);
            return nodes.size();
        } catch (Exception e) {
            throw new KafkaHealthCheckException("Failed to get broker count", e);
        }
    }

    /**
     * Cluster ID를 반환합니다.
     *
     * @throws KafkaHealthCheckException 연결 실패 시
     */
    public String clusterId() {
        try {
            return adminClient.describeCluster()
                    .clusterId().get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            throw new KafkaHealthCheckException("Failed to get cluster ID", e);
        }
    }

    /**
     * 컨트롤러 노드 정보를 반환합니다.
     *
     * @throws KafkaHealthCheckException 연결 실패 시
     */
    public Node controller() {
        try {
            return adminClient.describeCluster()
                    .controller().get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            throw new KafkaHealthCheckException("Failed to get controller node", e);
        }
    }

    @Override
    public void close() {
        adminClient.close(Duration.ofMillis(timeoutMs));
    }

    public static class KafkaHealthCheckException extends RuntimeException {
        public KafkaHealthCheckException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
