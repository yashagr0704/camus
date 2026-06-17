package com.kafkaclone;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * One of these exists per (topic, partition, consumerName) triple,
 * created lazily. consume()/ack() are synchronized on THIS object, not
 * on the whole Broker -- a lock per (partition, consumer) pair instead
 * of one lock for everything, so unrelated consumers never wait on
 * each other.
 */
public class PartitionConsumer {

    private final CommitLog log;
    private final ConsumerOffsetStore offsetStore;
    private PendingDelivery pending; // in-memory only, never persisted

    public PartitionConsumer(CommitLog log, ConsumerOffsetStore offsetStore) {
        this.log = log;
        this.offsetStore = offsetStore;
    }

    public synchronized ConsumeResult consume() throws IOException {
        if (pending != null) {
            return new ConsumeResult(pending.message(), true);
        }

        long committedOffset = offsetStore.load();
        if (committedOffset >= log.endOffset()) {
            return null; // caught up
        }

        byte[] payload = log.read(committedOffset);
        String message = new String(payload, StandardCharsets.UTF_8);
        long nextOffset = committedOffset + 4 + payload.length;

        pending = new PendingDelivery(message, nextOffset);
        return new ConsumeResult(message, false);
    }

    public synchronized boolean ack() throws IOException {
        if (pending == null) {
            return false;
        }
        offsetStore.save(pending.nextOffset());
        pending = null;
        return true;
    }

    private record PendingDelivery(String message, long nextOffset) {}

    public record ConsumeResult(String message, boolean redelivered) {}
}