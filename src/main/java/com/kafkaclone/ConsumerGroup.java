package com.kafkaclone;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConsumerGroup {

    private final int partitionCount;
    private final MetadataStore metadataStore;
    private final String topic;
    private final String groupName;
    private Map<String, List<Integer>> assignment = Map.of();

    public ConsumerGroup(int partitionCount, MetadataStore metadataStore, String topic, String groupName) throws SQLException {
        this.partitionCount = partitionCount;
        this.metadataStore = metadataStore;
        this.topic = topic;
        this.groupName = groupName;
        rebalance(); // pick up any membership that already existed in the database
    }

    public synchronized List<Integer> join(String memberId) throws SQLException {
        boolean changed = metadataStore.addGroupMember(topic, groupName, memberId);
        if (changed) {
            rebalance();
        }
        return assignment.getOrDefault(memberId, List.of());
    }

    public synchronized void leave(String memberId) throws SQLException {
        boolean changed = metadataStore.removeGroupMember(topic, groupName, memberId);
        if (changed) {
            rebalance();
        }
    }

    public synchronized List<Integer> assignmentFor(String memberId) {
        return assignment.getOrDefault(memberId, List.of());
    }

    private void rebalance() throws SQLException {
        List<String> sortedMembers = metadataStore.membersOf(topic, groupName); // already sorted by the SQL itself
        if (sortedMembers.isEmpty()) {
            assignment = Map.of();
            return;
        }
        Map<String, List<Integer>> next = new HashMap<>();
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