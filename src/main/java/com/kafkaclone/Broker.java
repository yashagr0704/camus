package com.kafkaclone;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A topic is now PARTITION_COUNT independent logs, not one. This class
 * is no longer synchronized anywhere -- there's no single shared lock
 * left. Real locking happens inside CommitLog (per partition) and
 * PartitionConsumer (per partition+consumer); this class only handles
 * looking up or lazily creating the right one, which ConcurrentHashMap
 * makes safe on its own.
 */
public class Broker {

    private static final int PARTITION_COUNT = 3;

    private final Map<String, CommitLog> partitionLogs = new ConcurrentHashMap<>();
    private final Map<String, PartitionConsumer> consumers = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> roundRobinCounters = new ConcurrentHashMap<>();
    private final String dataDirectory;

    public Broker(String dataDirectory) {
        this.dataDirectory = dataDirectory;
    }

    /** No key: spread messages evenly across partitions, in turn. */
    public PublishResult publish(String topic, String message) throws IOException {
        int partition = nextRoundRobinPartition(topic);
        return publishToPartition(topic, partition, message);
    }

    /** Key given: every message with this exact key always lands on the same partition. */
    public PublishResult publish(String topic, String key, String message) throws IOException {
        int partition = partitionForKey(key);
        return publishToPartition(topic, partition, message);
    }

    public PartitionConsumer.ConsumeResult consume(String topic, int partition, String consumerName) throws IOException {
        return consumerFor(topic, partition, consumerName).consume();
    }

    public boolean ack(String topic, int partition, String consumerName) throws IOException {
        return consumerFor(topic, partition, consumerName).ack();
    }

    private PublishResult publishToPartition(String topic, int partition, String message) throws IOException {
        long offset = logFor(topic, partition).append(message.getBytes(StandardCharsets.UTF_8));
        return new PublishResult(partition, offset);
    }

    // Math.floorMod, not plain %: hashCode() can be negative, and %
    // keeps the sign of its left operand, so naive "hash % count" can
    // itself come out negative. floorMod always lands in [0, count).
    private int partitionForKey(String key) {
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
        String partitionKey = topic + "-" + partition;
        String key = partitionKey + "::" + consumerName;
        return consumers.computeIfAbsent(key, k -> {
            CommitLog log = logFor(topic, partition);
            ConsumerOffsetStore store = new ConsumerOffsetStore(dataDirectory, partitionKey, consumerName);
            return new PartitionConsumer(log, store);
        });
    }

    private CommitLog openLog(String fileBaseName) {
        try {
            return new CommitLog(dataDirectory + "/" + fileBaseName + ".log");
        } catch (IOException e) {
            // computeIfAbsent's function can't declare checked exceptions,
            // so we wrap it -- Server's catch-all handles this surface.
            throw new RuntimeException("failed to open log for " + fileBaseName, e);
        }
    }

    public record PublishResult(int partition, long offset) {}
}