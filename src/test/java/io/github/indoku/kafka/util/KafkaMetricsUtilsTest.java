package io.github.indoku.kafka.util;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class KafkaMetricsUtilsTest {

    @Mock
    private AdminClient adminClient;

    private KafkaMetricsUtils metricsUtils;

    @BeforeEach
    void setUp() {
        metricsUtils = new KafkaMetricsUtils(adminClient);
    }

    @Test
    void committedOffsets_returnsPartitionOffsets() throws Exception {
        TopicPartition tp0 = new TopicPartition("my-topic", 0);
        TopicPartition tp1 = new TopicPartition("my-topic", 1);

        Map<TopicPartition, OffsetAndMetadata> offsets = new LinkedHashMap<>();
        offsets.put(tp0, new OffsetAndMetadata(100L));
        offsets.put(tp1, new OffsetAndMetadata(200L));

        ListConsumerGroupOffsetsResult mockResult = mock(ListConsumerGroupOffsetsResult.class);
        when(mockResult.partitionsToOffsetAndMetadata()).thenReturn(KafkaFuture.completedFuture(offsets));
        when(adminClient.listConsumerGroupOffsets("my-group")).thenReturn(mockResult);

        Map<TopicPartition, Long> result = metricsUtils.committedOffsets("my-group");

        assertEquals(2, result.size());
        assertEquals(100L, result.get(tp0));
        assertEquals(200L, result.get(tp1));
    }

    @Test
    void committedOffsets_returnsEmpty_whenNoOffsets() throws Exception {
        Map<TopicPartition, OffsetAndMetadata> offsets = Collections.emptyMap();

        ListConsumerGroupOffsetsResult mockResult = mock(ListConsumerGroupOffsetsResult.class);
        when(mockResult.partitionsToOffsetAndMetadata()).thenReturn(KafkaFuture.completedFuture(offsets));
        when(adminClient.listConsumerGroupOffsets("empty-group")).thenReturn(mockResult);

        Map<TopicPartition, Long> result = metricsUtils.committedOffsets("empty-group");

        assertTrue(result.isEmpty());
    }

    @Test
    void listConsumerGroups_returnsGroupIds() throws Exception {
        Collection<ConsumerGroupListing> listings = List.of(
                new ConsumerGroupListing("group-a", false),
                new ConsumerGroupListing("group-b", false)
        );

        ListConsumerGroupsResult mockResult = mock(ListConsumerGroupsResult.class);
        when(mockResult.all()).thenReturn(KafkaFuture.completedFuture(listings));
        when(adminClient.listConsumerGroups()).thenReturn(mockResult);

        Set<String> groups = metricsUtils.listConsumerGroups();

        assertEquals(2, groups.size());
        assertTrue(groups.contains("group-a"));
        assertTrue(groups.contains("group-b"));
    }
}
