package com.kafkaclone;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;


public class FollowerServer {

    private static final String LEADER_HOST = "localhost";
    private static final int LEADER_PORT = 9092;
    private static final int MY_PORT = 9093;
    private static final String DATA_DIR = "data-follower";
    private static final String REPLICATED_TOPIC = "orders";
    private static final int PARTITION_COUNT = 3; // must match the leader's
    private static final int POLL_DELAY_MS = 200;

    public static void main(String[] args) throws IOException, SQLException {
        Files.createDirectories(Path.of(DATA_DIR));
        Broker followerBroker = new Broker(DATA_DIR);

        for (int partition = 0; partition < PARTITION_COUNT; partition++) {
            final int p = partition;
            Thread replicationThread = new Thread(() -> replicatePartition(REPLICATED_TOPIC, p, followerBroker));
            replicationThread.setDaemon(true);
            replicationThread.start();
        }

        System.out.println("Follower replicating topic '" + REPLICATED_TOPIC + "' from "
                + LEADER_HOST + ":" + LEADER_PORT + ", also serving clients on port " + MY_PORT);

        // Also a fully normal server -- connect a Client to MY_PORT and
        // CONSUME directly to prove the replicated data is really here.
        Server.runServer(MY_PORT, followerBroker);
    }

    private static void replicatePartition(String topic, int partition, Broker followerBroker) {
        long localOffset = 0;

        while (true) {
            try (Socket socket = new Socket(LEADER_HOST, LEADER_PORT);
                 DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                 DataInputStream in = new DataInputStream(socket.getInputStream())) {

                while (true) {
                    Framing.writeFrame(out, "FETCH " + topic + " " + partition + " " + localOffset);
                    String response = Framing.readFrame(in);

                    if (response == null) {
                        break; // leader connection dropped -- reconnect on outer loop
                    }
                    if (response.equals("EOF")) {
                        Thread.sleep(POLL_DELAY_MS);
                        continue;
                    }
                    if (response.startsWith("RECORD")) {
                        String[] respParts = response.split(" ", 3);
                        long nextOffset = Long.parseLong(respParts[1]);
                        String payload = respParts[2];

                        followerBroker.appendReplicated(topic, partition, payload);
                        localOffset = nextOffset;

                        Framing.writeFrame(out, "REPLICATED " + topic + " " + partition + " " + localOffset);
                        Framing.readFrame(in); // discard the leader's "OK"
                    }
                }

            } catch (Exception e) {
                System.out.println("[replica " + topic + "-" + partition + "] lost leader, retrying: " + e.getMessage());
                try {
                    Thread.sleep(POLL_DELAY_MS);
                } catch (InterruptedException ignored) {}
            }
        }
    }
}