package io.github.indoku.kafka.util;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class KafkaTopicUtilsTest {

    @Mock AdminClient adminClient;
    @Mock ListTopicsResult listTopicsResult;
    @Mock DescribeTopicsResult describeTopicsResult;
    @Mock CreateTopicsResult createTopicsResult;
    @Mock DeleteTopicsResult deleteTopicsResult;

    private KafkaTopicUtils topicUtils;

    @BeforeEach
    void setUp() {
        topicUtils = new KafkaTopicUtils(adminClient);
    }

    // -------------------------------------------------------------------------
    // exists
    // -------------------------------------------------------------------------

    @Test
    void exists_returnsTrue_whenTopicPresent() throws Exception {
        when(adminClient.listTopics()).thenReturn(listTopicsResult);
        when(listTopicsResult.names()).thenReturn(KafkaFuture.completedFuture(Set.of("my-topic")));

        assertTrue(topicUtils.exists("my-topic"));
    }

    @Test
    void exists_returnsFalse_whenTopicAbsent() throws Exception {
        when(adminClient.listTopics()).thenReturn(listTopicsResult);
        when(listTopicsResult.names()).thenReturn(KafkaFuture.completedFuture(Set.of()));

        assertFalse(topicUtils.exists("ghost-topic"));
    }

    // -------------------------------------------------------------------------
    // partitionCount
    // -------------------------------------------------------------------------

    @Test
    void partitionCount_returnsCorrectCount() throws Exception {
        TopicPartitionInfo p0 = mock(TopicPartitionInfo.class);
        TopicPartitionInfo p1 = mock(TopicPartitionInfo.class);
        TopicPartitionInfo p2 = mock(TopicPartitionInfo.class);
        TopicDescription desc = new TopicDescription("my-topic", false, List.of(p0, p1, p2));

        when(adminClient.describeTopics(anyCollection())).thenReturn(describeTopicsResult);
        when(describeTopicsResult.allTopicNames()).thenReturn(KafkaFuture.completedFuture(Map.of("my-topic", desc)));

        assertEquals(3, topicUtils.partitionCount("my-topic"));
    }

    // -------------------------------------------------------------------------
    // listTopics
    // -------------------------------------------------------------------------

    @Test
    void listTopics_returnsAllTopics() throws Exception {
        Set<String> expected = Set.of("topic-a", "topic-b");
        when(adminClient.listTopics()).thenReturn(listTopicsResult);
        when(listTopicsResult.names()).thenReturn(KafkaFuture.completedFuture(expected));

        assertEquals(expected, topicUtils.listTopics());
    }

    // -------------------------------------------------------------------------
    // createIfAbsent
    // -------------------------------------------------------------------------

    @Test
    void createIfAbsent_createsAndReturnsTrue_whenTopicAbsent() throws Exception {
        when(adminClient.listTopics()).thenReturn(listTopicsResult);
        when(listTopicsResult.names()).thenReturn(KafkaFuture.completedFuture(Set.of()));
        when(adminClient.createTopics(any())).thenReturn(createTopicsResult);
        when(createTopicsResult.all()).thenReturn(KafkaFuture.completedFuture(null));

        assertTrue(topicUtils.createIfAbsent("new-topic", 3, (short) 1));
        verify(adminClient).createTopics(any());
    }

    @Test
    void createIfAbsent_returnsFalse_whenTopicAlreadyExists() throws Exception {
        when(adminClient.listTopics()).thenReturn(listTopicsResult);
        when(listTopicsResult.names()).thenReturn(KafkaFuture.completedFuture(Set.of("existing-topic")));

        assertFalse(topicUtils.createIfAbsent("existing-topic", 3, (short) 1));
        verify(adminClient, never()).createTopics(any());
    }

    // -------------------------------------------------------------------------
    // delete
    // -------------------------------------------------------------------------

    @Test
    void delete_callsAdminClient() throws Exception {
        when(adminClient.deleteTopics(anyCollection())).thenReturn(deleteTopicsResult);
        when(deleteTopicsResult.all()).thenReturn(KafkaFuture.completedFuture(null));

        assertDoesNotThrow(() -> topicUtils.delete("my-topic"));
        verify(adminClient).deleteTopics(Collections.singleton("my-topic"));
    }

    // -------------------------------------------------------------------------
    // close
    // -------------------------------------------------------------------------

    @Test
    void close_closesAdminClient() {
        topicUtils.close();
        verify(adminClient).close();
    }
}
