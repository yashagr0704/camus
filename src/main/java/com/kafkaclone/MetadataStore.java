package com.kafkaclone;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;


public class MetadataStore implements AutoCloseable {

    private final Connection connection;

    public MetadataStore(String dataDirectory) throws SQLException {
        connection = DriverManager.getConnection("jdbc:sqlite:" + dataDirectory + "/metadata.db");

        try (Statement stmt = connection.createStatement()) {
            // WAL mode lets readers proceed without blocking on an
            // in-progress writer; the default mode locks the whole file
            // for the duration of a write.
            stmt.execute("PRAGMA journal_mode=WAL");

            stmt.execute("""
                CREATE TABLE IF NOT EXISTS consumer_offsets (
                    topic TEXT NOT NULL,
                    partition INTEGER NOT NULL,
                    consumer_name TEXT NOT NULL,
                    committed_offset INTEGER NOT NULL,
                    PRIMARY KEY (topic, partition, consumer_name)
                )
                """);

            stmt.execute("""
                CREATE TABLE IF NOT EXISTS group_members (
                    topic TEXT NOT NULL,
                    group_name TEXT NOT NULL,
                    member_id TEXT NOT NULL,
                    PRIMARY KEY (topic, group_name, member_id)
                )
                """);
        }
    }

    public synchronized long loadOffset(String topic, int partition, String consumerName) throws SQLException {
        String sql = "SELECT committed_offset FROM consumer_offsets WHERE topic = ? AND partition = ? AND consumer_name = ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, topic);
            ps.setInt(2, partition);
            ps.setString(3, consumerName);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong("committed_offset") : 0L;
            }
        }
    }

    public synchronized void saveOffset(String topic, int partition, String consumerName, long offset) throws SQLException {
        String sql = """
            INSERT INTO consumer_offsets (topic, partition, consumer_name, committed_offset)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (topic, partition, consumer_name)
            DO UPDATE SET committed_offset = excluded.committed_offset
            """;
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, topic);
            ps.setInt(2, partition);
            ps.setString(3, consumerName);
            ps.setLong(4, offset);
            ps.executeUpdate();
        }
    }

    public synchronized boolean addGroupMember(String topic, String groupName, String memberId) throws SQLException {
        String sql = "INSERT OR IGNORE INTO group_members (topic, group_name, member_id) VALUES (?, ?, ?)";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, topic);
            ps.setString(2, groupName);
            ps.setString(3, memberId);
            return ps.executeUpdate() > 0;
        }
    }

    public synchronized boolean removeGroupMember(String topic, String groupName, String memberId) throws SQLException {
        String sql = "DELETE FROM group_members WHERE topic = ? AND group_name = ? AND member_id = ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, topic);
            ps.setString(2, groupName);
            ps.setString(3, memberId);
            return ps.executeUpdate() > 0;
        }
    }

    public synchronized List<String> membersOf(String topic, String groupName) throws SQLException {
        String sql = "SELECT member_id FROM group_members WHERE topic = ? AND group_name = ? ORDER BY member_id";
        List<String> members = new ArrayList<>();
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, topic);
            ps.setString(2, groupName);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    members.add(rs.getString("member_id"));
                }
            }
        }
        return members;
    }

    @Override
    public void close() throws SQLException {
        connection.close();
    }
}