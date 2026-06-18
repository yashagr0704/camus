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

/**
 * PHASE 1 -- runs forever, accepting client after client.
 * PHASE 2/3 -- PUBLISH/CONSUME/ACK are backed by a real Broker instead
 * of toy in-memory responses.
 * PHASE 4 -- communication uses length-prefixed binary framing
 * (Framing.java) instead of readLine()/println().
 * PHASE 5 -- each accepted connection is handed off to its own virtual
 * thread, so one slow client can no longer block every other client
 * from connecting.
 */
public class Server {

    private static final int PORT = 9092;
    private static final String DATA_DIR = "data";

    public static void main(String[] args) throws IOException, SQLException {
        Files.createDirectories(Path.of(DATA_DIR));
        Broker broker = new Broker(DATA_DIR);

        try (ServerSocket serverSocket = new ServerSocket(PORT);
             ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {

            System.out.println("Server listening on port " + PORT + "...");

            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("Client connected: " + clientSocket.getRemoteSocketAddress());

                // submit() hands this connection's whole lifetime off to a
                // brand-new virtual thread and returns immediately -- the
                // loop comes straight back to accept() without waiting
                // for this client to do anything at all.
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

    /**
     * The whole "protocol": one frame of text, first word is the command
     * name, everything after it is the argument.
     */
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
                    // round-robin: "<topic> <message>"
                    String[] pubParts = argument.split(" ", 2);
                    if (pubParts.length < 2) {
                        return "ERROR: usage PUBLISH <topic> <message>";
                    }
                    Broker.PublishResult result = broker.publish(pubParts[0], pubParts[1]);
                    return "OK " + result.partition() + " " + result.offset();
                }

                case "PUBLISHKEY": {
                    // sticky-by-key: "<topic> <key> <message>"
                    String[] pkParts = argument.split(" ", 3);
                    if (pkParts.length < 3) {
                        return "ERROR: usage PUBLISHKEY <topic> <key> <message>";
                    }
                    Broker.PublishResult result = broker.publish(pkParts[0], pkParts[1], pkParts[2]);
                    return "OK " + result.partition() + " " + result.offset();
                }

                case "CONSUME": {
                    // now requires the partition explicitly: "<topic> <partition> <consumerName>"
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

                case "JOIN": {
                    // "<topic> <groupName> <memberId>"
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
                    // "<topic> <groupName> <memberId>" -- looks up which partition(s)
                    // this member currently owns, and returns the next available
                    // message from among them, tagged with which partition it actually
                    // came from (you'll need that exact number to ACK afterward).
                    String[] cgParts = argument.split(" ", 3);
                    if (cgParts.length < 3) {
                        return "ERROR: usage CONSUMEGROUP <topic> <groupName> <memberId>";
                    }
                    String topic = cgParts[0];
                    String groupName = cgParts[1];
                    String memberId = cgParts[2];

                    List<Integer> ownedPartitions = broker.assignmentFor(topic, groupName, memberId);
                    if (ownedPartitions.isEmpty()) {
                        return "EOF"; // this member currently owns nothing at all
                    }

                    for (int partition : ownedPartitions) {
                        PartitionConsumer.ConsumeResult result = broker.consume(topic, partition, groupName);
                        if (result != null) {
                            String tag = result.redelivered() ? "REDELIVERED" : "NEW";
                            return "MESSAGE " + partition + " " + tag + " " + result.message();
                        }
                    }
                    return "EOF"; // every partition this member owns is caught up
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
        } catch (RuntimeException e) {
            return "ERROR: " + e.getMessage();
        } catch (SQLException e) {
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
