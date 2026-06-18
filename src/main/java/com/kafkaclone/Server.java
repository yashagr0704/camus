package com.kafkaclone;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Server {

    private static final int PORT = 9092;
    private static final String DATA_DIR = "data";

    public static void main(String[] args) throws IOException, SQLException {
        Files.createDirectories(Path.of(DATA_DIR));
        Broker broker = new Broker(DATA_DIR);
        runServer(PORT, broker);
    }

    /** Pulled out so FollowerServer can run the exact same accept loop on its own port/broker. */
    public static void runServer(int port, Broker broker) throws IOException {
        try (ServerSocket serverSocket = new ServerSocket(port);
             ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {

            System.out.println("Server listening on port " + port + "...");

            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("Client connected: " + clientSocket.getRemoteSocketAddress());

                executor.submit(() -> {
                    try {
                        handleClient(clientSocket, broker);
                    } catch (IOException e) {
                        System.out.println("Connection error: " + e.getMessage());
                    }
                    System.out.println("Client disconnected: " + clientSocket.getRemoteSocketAddress());
                });
            }
        }
    }

    private static void handleClient(Socket clientSocket, Broker broker) throws IOException {
        try (clientSocket;
             DataInputStream in = new DataInputStream(clientSocket.getInputStream());
             DataOutputStream out = new DataOutputStream(clientSocket.getOutputStream())) {

            String line;
            while ((line = Framing.readFrame(in)) != null) {
                String response = handleCommand(line, broker);
                Framing.writeFrame(out, response);

                if (line.trim().equalsIgnoreCase("QUIT")) {
                    break;
                }
            }
        }
    }

    private static String handleCommand(String line, Broker broker) {
        String trimmed = line.trim();
        String[] parts = trimmed.split(" ", 2);
        String command = parts[0].toUpperCase();
        String argument = parts.length > 1 ? parts[1] : "";

        try {
            switch (command) {
                case "PING":
                    return "PONG";

                case "PUBLISH": {
                    String[] pubParts = argument.split(" ", 2);
                    if (pubParts.length < 2) {
                        return "ERROR: usage PUBLISH <topic> <message>";
                    }
                    Broker.PublishResult result = broker.publish(pubParts[0], pubParts[1]);
                    return "OK " + result.partition() + " " + result.offset();
                }

                case "PUBLISHKEY": {
                    String[] pkParts = argument.split(" ", 3);
                    if (pkParts.length < 3) {
                        return "ERROR: usage PUBLISHKEY <topic> <key> <message>";
                    }
                    Broker.PublishResult result = broker.publish(pkParts[0], pkParts[1], pkParts[2]);
                    return "OK " + result.partition() + " " + result.offset();
                }

                case "PUBLISHACKALL": {
                    // "<topic> <message>" -- blocks until a follower
                    // confirms it has this exact record, or fails after
                    // a fixed timeout. The visible CAP trade-off.
                    String[] paaParts = argument.split(" ", 2);
                    if (paaParts.length < 2) {
                        return "ERROR: usage PUBLISHACKALL <topic> <message>";
                    }
                    Broker.PublishResult result = broker.publishWithAck(paaParts[0], paaParts[1], 5000);
                    return "OK " + result.partition() + " " + result.offset();
                }

                case "PARTITIONFOR": {
                    String[] pfParts = argument.split(" ", 2);
                    if (pfParts.length < 2) {
                        return "ERROR: usage PARTITIONFOR <topic> <key>";
                    }
                    int partition = broker.partitionForKey(pfParts[1]);
                    return "PARTITION " + partition;
                }

                case "CONSUME": {
                    String[] conParts = argument.split(" ", 3);
                    if (conParts.length < 3) {
                        return "ERROR: usage CONSUME <topic> <partition> <consumerName>";
                    }
                    int partition = Integer.parseInt(conParts[1]);
                    PartitionConsumer.ConsumeResult result = broker.consume(conParts[0], partition, conParts[2]);
                    if (result == null) {
                        return "EOF";
                    }
                    String tag = result.redelivered() ? "REDELIVERED" : "NEW";
                    return "MESSAGE " + tag + " " + result.message();
                }

                case "ACK": {
                    String[] ackParts = argument.split(" ", 3);
                    if (ackParts.length < 3) {
                        return "ERROR: usage ACK <topic> <partition> <consumerName>";
                    }
                    int partition = Integer.parseInt(ackParts[1]);
                    boolean acked = broker.ack(ackParts[0], partition, ackParts[2]);
                    return acked ? "OK" : "ERROR: nothing to acknowledge";
                }

                case "CONSUMEKEY": {
                    String[] ckParts = argument.split(" ", 3);
                    if (ckParts.length < 3) {
                        return "ERROR: usage CONSUMEKEY <topic> <key> <consumerName>";
                    }
                    int partition = broker.partitionForKey(ckParts[1]);
                    PartitionConsumer.ConsumeResult result = broker.consume(ckParts[0], partition, ckParts[2]);
                    if (result == null) {
                        return "EOF";
                    }
                    String tag = result.redelivered() ? "REDELIVERED" : "NEW";
                    return "MESSAGE " + tag + " " + result.message();
                }

                case "ACKKEY": {
                    String[] akParts = argument.split(" ", 3);
                    if (akParts.length < 3) {
                        return "ERROR: usage ACKKEY <topic> <key> <consumerName>";
                    }
                    int partition = broker.partitionForKey(akParts[1]);
                    boolean acked = broker.ack(akParts[0], partition, akParts[2]);
                    return acked ? "OK" : "ERROR: nothing to acknowledge";
                }

                case "JOIN": {
                    String[] joinParts = argument.split(" ", 3);
                    if (joinParts.length < 3) {
                        return "ERROR: usage JOIN <topic> <groupName> <memberId>";
                    }
                    List<Integer> assigned = broker.joinGroup(joinParts[0], joinParts[1], joinParts[2]);
                    return "ASSIGNED " + formatPartitions(assigned);
                }

                case "LEAVE": {
                    String[] leaveParts = argument.split(" ", 3);
                    if (leaveParts.length < 3) {
                        return "ERROR: usage LEAVE <topic> <groupName> <memberId>";
                    }
                    broker.leaveGroup(leaveParts[0], leaveParts[1], leaveParts[2]);
                    return "OK";
                }

                case "ASSIGNMENT": {
                    String[] asParts = argument.split(" ", 3);
                    if (asParts.length < 3) {
                        return "ERROR: usage ASSIGNMENT <topic> <groupName> <memberId>";
                    }
                    List<Integer> assigned = broker.assignmentFor(asParts[0], asParts[1], asParts[2]);
                    return "ASSIGNED " + formatPartitions(assigned);
                }

                case "CONSUMEGROUP": {
                    String[] cgParts = argument.split(" ", 3);
                    if (cgParts.length < 3) {
                        return "ERROR: usage CONSUMEGROUP <topic> <groupName> <memberId>";
                    }
                    String topic = cgParts[0];
                    String groupName = cgParts[1];
                    String memberId = cgParts[2];

                    List<Integer> ownedPartitions = broker.assignmentFor(topic, groupName, memberId);
                    if (ownedPartitions.isEmpty()) {
                        return "EOF";
                    }
                    for (int partition : ownedPartitions) {
                        PartitionConsumer.ConsumeResult result = broker.consume(topic, partition, groupName);
                        if (result != null) {
                            String tag = result.redelivered() ? "REDELIVERED" : "NEW";
                            return "MESSAGE " + partition + " " + tag + " " + result.message();
                        }
                    }
                    return "EOF";
                }

                case "FETCH": {
                    // "<topic> <partition> <offset>" -- raw read, no
                    // consumer bookkeeping. What followers use.
                    String[] fetchParts = argument.split(" ", 3);
                    if (fetchParts.length < 3) {
                        return "ERROR: usage FETCH <topic> <partition> <offset>";
                    }
                    int partition = Integer.parseInt(fetchParts[1]);
                    long offset = Long.parseLong(fetchParts[2]);
                    Broker.FetchResult result = broker.fetch(fetchParts[0], partition, offset);
                    if (result == null) {
                        return "EOF";
                    }
                    return "RECORD " + result.nextOffset() + " " + result.message();
                }

                case "REPLICATED": {
                    // "<topic> <partition> <offset>" -- a follower
                    // reporting how far it's caught up, unblocking any
                    // PUBLISHACKALL waiting on it.
                    String[] repParts = argument.split(" ", 3);
                    if (repParts.length < 3) {
                        return "ERROR: usage REPLICATED <topic> <partition> <offset>";
                    }
                    int partition = Integer.parseInt(repParts[1]);
                    long offset = Long.parseLong(repParts[2]);
                    broker.reportReplicated(repParts[0], partition, offset);
                    return "OK";
                }

                case "QUIT":
                    return "BYE";

                default:
                    return "ERROR: unknown command '" + command + "'";
            }
        } catch (IOException e) {
            return "ERROR: " + e.getMessage();
        } catch (NumberFormatException e) {
            return "ERROR: bad argument";
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return "ERROR: interrupted while waiting for replication";
        } catch (SQLException e) {
            return "ERROR: " + e.getMessage();
        } catch (RuntimeException e) {
            return "ERROR: " + e.getMessage();
        }
    }

    private static String formatPartitions(List<Integer> partitions) {
        if (partitions.isEmpty()) {
            return "NONE";
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < partitions.size(); i++) {
            if (i > 0) sb.append(",");
            sb.append(partitions.get(i));
        }
        return sb.toString();
    }
}