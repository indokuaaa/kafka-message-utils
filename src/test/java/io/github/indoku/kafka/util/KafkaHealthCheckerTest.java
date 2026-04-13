package io.github.indoku.kafka.util;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class KafkaHealthCheckerTest {

    @Mock
    private AdminClient adminClient;

    @Mock
    private DescribeClusterResult describeClusterResult;

    private KafkaHealthChecker checker;

    @BeforeEach
    void setUp() {
        checker = new KafkaHealthChecker(adminClient, 3000L);
    }

    @Test
    void isHealthy_returnsTrue_whenBrokerResponds() throws Exception {
        Collection<Node> nodes = List.of(new Node(0, "localhost", 9092));
        KafkaFuture<Collection<Node>> future = KafkaFuture.completedFuture(nodes);

        when(adminClient.describeCluster()).thenReturn(describeClusterResult);
        when(describeClusterResult.nodes()).thenReturn(future);

        assertTrue(checker.isHealthy());
    }

    @Test
    void isHealthy_returnsFalse_whenBrokerUnreachable() {
        when(adminClient.describeCluster()).thenThrow(new RuntimeException("Connection refused"));

        assertFalse(checker.isHealthy());
    }

    @Test
    void brokerCount_returnsNumberOfNodes() throws Exception {
        Collection<Node> nodes = List.of(
                new Node(0, "broker-1", 9092),
                new Node(1, "broker-2", 9092),
                new Node(2, "broker-3", 9092)
        );
        KafkaFuture<Collection<Node>> future = KafkaFuture.completedFuture(nodes);

        when(adminClient.describeCluster()).thenReturn(describeClusterResult);
        when(describeClusterResult.nodes()).thenReturn(future);

        assertEquals(3, checker.brokerCount());
    }

    @Test
    void brokerCount_throwsException_whenFails() {
        when(adminClient.describeCluster()).thenThrow(new RuntimeException("timeout"));

        assertThrows(KafkaHealthChecker.KafkaHealthCheckException.class, () -> checker.brokerCount());
    }

    @Test
    void clusterId_returnsId() throws Exception {
        KafkaFuture<String> future = KafkaFuture.completedFuture("test-cluster-id");

        when(adminClient.describeCluster()).thenReturn(describeClusterResult);
        when(describeClusterResult.clusterId()).thenReturn(future);

        assertEquals("test-cluster-id", checker.clusterId());
    }

    @Test
    void controller_returnsNode() throws Exception {
        Node controllerNode = new Node(0, "controller-host", 9092);
        KafkaFuture<Node> future = KafkaFuture.completedFuture(controllerNode);

        when(adminClient.describeCluster()).thenReturn(describeClusterResult);
        when(describeClusterResult.controller()).thenReturn(future);

        Node result = checker.controller();
        assertEquals("controller-host", result.host());
        assertEquals(9092, result.port());
    }
}
