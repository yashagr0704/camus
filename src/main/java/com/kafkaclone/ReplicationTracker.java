package com.kafkaclone;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * PHASE 9 -- lives on the LEADER. Tracks, per partition, the highest
 * offset any follower has confirmed it has replicated. PUBLISHACKALL
 * polls this until the just-written offset is covered, or times out.
 */
public class ReplicationTracker {

    private final Map<String, Long> replicatedOffsets = new ConcurrentHashMap<>();

    public void reportReplicated(String topic, int partition, long offset) {
        String key = topic + "-" + partition;
        // merge with Math::max so an out-of-order or stale report can
        // never make replication progress appear to go backwards.
        replicatedOffsets.merge(key, offset, Math::max);
    }

    public long replicatedOffsetFor(String topic, int partition) {
        return replicatedOffsets.getOrDefault(topic + "-" + partition, 0L);
    }

    /** Polls until the follower has caught up to targetOffset, or the timeout elapses. */
    public boolean waitForReplication(String topic, int partition, long targetOffset, long timeoutMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (replicatedOffsetFor(topic, partition) >= targetOffset) {
                return true;
            }
            Thread.sleep(20);
        }
        return replicatedOffsetFor(topic, partition) >= targetOffset; // one last check
    }
}