package com.kafkaclone;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;

public class PartitionConsumer {

    private final CommitLog log;
    private final MetadataStore metadataStore;
    private final String topic;
    private final int partition;
    private final String consumerName;
    private PendingDelivery pending;

    public PartitionConsumer(CommitLog log, MetadataStore metadataStore, String topic, int partition, String consumerName) {
        this.log = log;
        this.metadataStore = metadataStore;
        this.topic = topic;
        this.partition = partition;
        this.consumerName = consumerName;
    }

    public synchronized ConsumeResult consume() throws IOException, SQLException {
        if (pending != null) {
            return new ConsumeResult(pending.message(), true);
        }

        long committedOffset = metadataStore.loadOffset(topic, partition, consumerName);
        if (committedOffset >= log.endOffset()) {
            return null;
        }

        byte[] payload = log.read(committedOffset);
        String message = new String(payload, StandardCharsets.UTF_8);
        long nextOffset = committedOffset + 4 + payload.length;

        pending = new PendingDelivery(message, nextOffset);
        return new ConsumeResult(message, false);
    }

    public synchronized boolean ack() throws IOException, SQLException {
        if (pending == null) {
            return false;
        }
        metadataStore.saveOffset(topic, partition, consumerName, pending.nextOffset());
        pending = null;
        return true;
    }

    private record PendingDelivery(String message, long nextOffset) {}

    public record ConsumeResult(String message, boolean redelivered) {}
}
