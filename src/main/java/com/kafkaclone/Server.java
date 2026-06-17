package com.kafkaclone;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
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

    public static void main(String[] args) throws IOException {
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
        }
    }
}
