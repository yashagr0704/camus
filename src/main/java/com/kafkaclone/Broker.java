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
    private final ReplicationTracker replicationTracker = new ReplicationTracker();
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

    /**
     * acks=all: writes locally exactly like publish(), then blocks until
     * a follower has confirmed it has this exact record, or throws if
     * the timeout elapses first -- the CAP trade-off, made visible.
     */
    public PublishResult publishWithAck(String topic, String message, long timeoutMillis) throws IOException, InterruptedException {
        PublishResult result = publish(topic, message);
        long targetOffset = result.offset() + 4 + message.getBytes(StandardCharsets.UTF_8).length;
        boolean caughtUp = replicationTracker.waitForReplication(topic, result.partition(), targetOffset, timeoutMillis);
        if (!caughtUp) {
            throw new IOException("timed out waiting for replication acknowledgment");
        }
        return result;
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

    /** Raw read by exact offset -- no consumer name, no pending/ack bookkeeping. What FETCH uses. */
    public FetchResult fetch(String topic, int partition, long offset) throws IOException {
        CommitLog log = logFor(topic, partition);
        if (offset >= log.endOffset()) {
            return null;
        }
        byte[] payload = log.read(offset);
        String message = new String(payload, StandardCharsets.UTF_8);
        long nextOffset = offset + 4 + payload.length;
        return new FetchResult(message, nextOffset);
    }

    /** Appends directly to a SPECIFIC partition with no partition selection -- what a follower uses to mirror exactly what the leader wrote. */
    public void appendReplicated(String topic, int partition, String message) throws IOException {
        logFor(topic, partition).append(message.getBytes(StandardCharsets.UTF_8));
    }

    public void reportReplicated(String topic, int partition, long offset) {
        replicationTracker.reportReplicated(topic, partition, offset);
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
    public record FetchResult(String message, long nextOffset) {}
}