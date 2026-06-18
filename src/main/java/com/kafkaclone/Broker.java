package com.kafkaclone;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class Broker {

    private static final int PARTITION_COUNT = 3;

    private final Map<String, CommitLog> partitionLogs = new ConcurrentHashMap<>();
    private final Map<String, PartitionConsumer> consumers = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> roundRobinCounters = new ConcurrentHashMap<>();
    private final Map<String, ConsumerGroup> groups = new ConcurrentHashMap<>();
    private final MetadataStore metadataStore;
    private final String dataDirectory;

    public Broker(String dataDirectory) throws SQLException {
        this.dataDirectory = dataDirectory;
        this.metadataStore = new MetadataStore(dataDirectory);
    }

    public PublishResult publish(String topic, String message) throws IOException {
        int partition = nextRoundRobinPartition(topic);
        return publishToPartition(topic, partition, message);
    }

    public PublishResult publish(String topic, String key, String message) throws IOException {
        int partition = partitionForKey(key);
        return publishToPartition(topic, partition, message);
    }

    public PartitionConsumer.ConsumeResult consume(String topic, int partition, String consumerName) throws IOException, SQLException {
        return consumerFor(topic, partition, consumerName).consume();
    }

    public boolean ack(String topic, int partition, String consumerName) throws IOException, SQLException {
        return consumerFor(topic, partition, consumerName).ack();
    }

    public List<Integer> joinGroup(String topic, String groupName, String memberId) throws SQLException {
        return groupFor(topic, groupName).join(memberId);
    }

    public void leaveGroup(String topic, String groupName, String memberId) throws SQLException {
        groupFor(topic, groupName).leave(memberId);
    }

    public List<Integer> assignmentFor(String topic, String groupName, String memberId) {
        return groupFor(topic, groupName).assignmentFor(memberId);
    }

    private PublishResult publishToPartition(String topic, int partition, String message) throws IOException {
        long offset = logFor(topic, partition).append(message.getBytes(StandardCharsets.UTF_8));
        return new PublishResult(partition, offset);
    }

    public int partitionForKey(String key) {
        return Math.floorMod(key.hashCode(), PARTITION_COUNT);
    }

    private int nextRoundRobinPartition(String topic) {
        AtomicInteger counter = roundRobinCounters.computeIfAbsent(topic, t -> new AtomicInteger(0));
        return Math.floorMod(counter.getAndIncrement(), PARTITION_COUNT);
    }

    private CommitLog logFor(String topic, int partition) {
        String key = topic + "-" + partition;
        return partitionLogs.computeIfAbsent(key, this::openLog);
    }

    private PartitionConsumer consumerFor(String topic, int partition, String consumerName) {
        String key = topic + "-" + partition + "::" + consumerName;
        return consumers.computeIfAbsent(key, k ->
                new PartitionConsumer(logFor(topic, partition), metadataStore, topic, partition, consumerName));
    }

    private ConsumerGroup groupFor(String topic, String groupName) {
        String key = topic + "::" + groupName;
        return groups.computeIfAbsent(key, k -> {
            try {
                return new ConsumerGroup(PARTITION_COUNT, metadataStore, topic, groupName);
            } catch (SQLException e) {
                throw new RuntimeException("failed to load group state for " + key, e);
            }
        });
    }

    private CommitLog openLog(String fileBaseName) {
        try {
            return new CommitLog(dataDirectory + "/" + fileBaseName + ".log");
        } catch (IOException e) {
            throw new RuntimeException("failed to open log for " + fileBaseName, e);
        }
    }

    public record PublishResult(int partition, long offset) {}
}