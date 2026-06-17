package com.kafkaclone;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * PHASE 4 -- length-prefixed framing: write a 4-byte length, then exactly
 * that many bytes. Replaces readLine()/println() used in Phases 1-3, so
 * messages can now contain newlines or arbitrary bytes safely.
 */
public final class Framing {

    // A sane upper bound so a bogus or malicious length header can't make
    // us try to allocate gigabytes of memory before validating anything.
    private static final int MAX_FRAME_SIZE = 10 * 1024 * 1024; // 10 MB

    private Framing() {}

    public static void writeFrame(DataOutputStream out, String text) throws IOException {
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
        out.flush();
    }

    /**
     * Reads exactly one frame, regardless of what bytes it contains.
     * Returns null if the connection closed cleanly between frames --
     * our way of detecting "the client disconnected."
     */
    public static String readFrame(DataInputStream in) throws IOException {
        int length;
        try {
            length = in.readInt();
        } catch (EOFException e) {
            return null; // clean disconnect between frames
        }

        if (length < 0 || length > MAX_FRAME_SIZE) {
            throw new IOException("frame length out of bounds: " + length);
        }

        byte[] bytes = new byte[length];
        in.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
