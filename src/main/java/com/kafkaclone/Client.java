package com.kafkaclone;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Scanner;

/**
 * Interactive client: reads commands you type and sends each one to the
 * server, printing whatever comes back. Type QUIT to end the session.
 */
public class Client {

    public static void main(String[] args) throws IOException {
        String host = "localhost";
        int port = 9092;

        try (Socket socket = new Socket(host, port);
             DataOutputStream out = new DataOutputStream(socket.getOutputStream());
             DataInputStream in = new DataInputStream(socket.getInputStream());
             Scanner keyboard = new Scanner(System.in)) {
            System.out.println("Connected to " + host + ":" + port
                    + ". Try: PING, PUBLISH orders hello, PUBLISHKEY orders user42 hello, CONSUME orders 0 alice, ACK orders 0 alice, QUIT");

            while (true) {
                System.out.print("> ");
                String userInput = keyboard.nextLine();

                Framing.writeFrame(out, userInput);
                String response = Framing.readFrame(in);
                System.out.println(response);

                if (userInput.trim().equalsIgnoreCase("QUIT")) {
                    break;
                }
            }
        }
    }
}
