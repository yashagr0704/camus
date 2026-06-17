package com.kafkaclone;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * PHASE 2/3 -- owns one CommitLog per topic and one ConsumerOffsetStore
 * per (topic, consumer) pair, plus an in-memory map of "delivered but
 * not yet acknowledged" messages, which gives us at-least-once delivery:
 * calling consume() again before ack() just redelivers the same message.
 *
 * PHASE 5 -- publish/consume/ack are now synchronized on this Broker
 * instance, making each one atomic as a whole now that multiple client
 * threads can call in concurrently. This is a deliberately coarse,
 * whole-broker lock; partitions (a later phase) will let us shard it so
 * unrelated topics/partitions stop blocking each other.
 */
public class Broker {

    private final Map<String, CommitLog> topicLogs = new HashMap<>();
    private final Map<String, ConsumerOffsetStore> offsetStores = new HashMap<>();
    private final Map<String, PendingDelivery> pendingDeliveries = new HashMap<>();
    private final String dataDirectory;

    public Broker(String dataDirectory) {
        this.dataDirectory = dataDirectory;
    }

    public synchronized long publish(String topic, String message) throws IOException {
        return logFor(topic).append(message.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Delivers the next unread message to a named consumer, or
     * redelivers the consumer's already-pending message if it hasn't
     * been acknowledged yet. Returns null if the consumer is caught up
     * to the end of the log.
     */
    public synchronized ConsumeResult consume(String topic, String consumerName) throws IOException {
        String key = key(topic, consumerName);

        PendingDelivery pending = pendingDeliveries.get(key);
        if (pending != null) {
            return new ConsumeResult(pending.message(), true);
        }

        long committedOffset = offsetStoreFor(topic, consumerName).load();
        CommitLog log = logFor(topic);

        if (committedOffset >= log.endOffset()) {
            return null; // caught up, nothing new yet
        }

        byte[] payload = log.read(committedOffset);
        String message = new String(payload, StandardCharsets.UTF_8);
        long nextOffset = committedOffset + 4 + payload.length;

        pendingDeliveries.put(key, new PendingDelivery(message, nextOffset));
        return new ConsumeResult(message, false);
    }

    /**
     * Confirms the consumer's currently pending message is processed:
     * the durable committed offset moves forward and the in-memory
     * pending marker clears.
     */
    public synchronized boolean ack(String topic, String consumerName) throws IOException {
        String key = key(topic, consumerName);
        PendingDelivery pending = pendingDeliveries.get(key);
        if (pending == null) {
            return false; // nothing outstanding to acknowledge
        }
        offsetStoreFor(topic, consumerName).save(pending.nextOffset());
        pendingDeliveries.remove(key);
        return true;
    }

    private String key(String topic, String consumerName) {
        return topic + "::" + consumerName;
    }

    private CommitLog logFor(String topic) throws IOException {
        CommitLog log = topicLogs.get(topic);
        if (log == null) {
            log = new CommitLog(dataDirectory + "/" + topic + ".log");
            topicLogs.put(topic, log);
        }
        return log;
    }

    private ConsumerOffsetStore offsetStoreFor(String topic, String consumerName) {
        return offsetStores.computeIfAbsent(
                key(topic, consumerName),
                k -> new ConsumerOffsetStore(dataDirectory, topic, consumerName));
    }

    private record PendingDelivery(String message, long nextOffset) {}

    public record ConsumeResult(String message, boolean redelivered) {}
}
