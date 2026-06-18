package com.kafkaclone;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * PHASE 7 -- tracks membership and partition assignment for ONE
 * (topic, groupName) pair. Every join or leave triggers a full
 * recompute of the assignment for ALL current members -- deliberately
 * naive, a real "rebalancing storm" in miniature, on purpose.
 */
public class ConsumerGroup {

    private final int partitionCount;
    private final TreeSet<String> members = new TreeSet<>(); // sorted for deterministic assignment
    private Map<String, List<Integer>> assignment = Map.of();

    public ConsumerGroup(int partitionCount) {
        this.partitionCount = partitionCount;
    }

    public synchronized List<Integer> join(String memberId) {
        boolean changed = members.add(memberId);
        if (changed) {
            rebalance(); // only disrupt everyone if membership actually changed
        }
        return assignment.getOrDefault(memberId, List.of());
    }

    public synchronized void leave(String memberId) {
        boolean changed = members.remove(memberId);
        if (changed) {
            rebalance();
        }
    }

    public synchronized List<Integer> assignmentFor(String memberId) {
        return assignment.getOrDefault(memberId, List.of());
    }

    /** Round-robin over the sorted member list -- the simplest possible assignor. */
    private void rebalance() {
        if (members.isEmpty()) {
            assignment = Map.of();
            return;
        }
        Map<String, List<Integer>> next = new HashMap<>();
        List<String> sortedMembers = new ArrayList<>(members);
        for (String member : sortedMembers) {
            next.put(member, new ArrayList<>());
        }
        for (int partition = 0; partition < partitionCount; partition++) {
            String owner = sortedMembers.get(partition % sortedMembers.size());
            next.get(owner).add(partition);
        }
        assignment = next;
    }
}