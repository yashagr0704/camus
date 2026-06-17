package com.kafkaclone;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

/**
 * PHASE 2 -- the append-only commit log.
 *
 * Each record on disk is stored as:
 *     [4 bytes: length of payload, big-endian int] [payload bytes]
 *
 * "Offset" here is the literal byte position where a record's
 * length-prefix begins, not a logical record number -- a deliberate
 * simplification versus real Kafka's logical-offset + index-file design.
 */
public class CommitLog implements AutoCloseable {

    private final RandomAccessFile file;

    public CommitLog(String path) throws IOException {
        this.file = new RandomAccessFile(path, "rw");
        // Start writing new records at the end of whatever's already
        // there, so restarting the process doesn't erase history.
        this.file.seek(this.file.length());
    }

    /**
     * Appends a record and returns the offset it was written at. Callers
     * use that returned offset to read this exact record back later.
     */
    public synchronized long append(byte[] payload) throws IOException {
        long offset = file.getFilePointer();

        ByteBuffer header = ByteBuffer.allocate(4);
        header.putInt(payload.length);

        file.write(header.array());
        file.write(payload);

        // force/sync == fsync: blocks until the OS has actually pushed
        // these bytes to physical storage, not just an in-memory page
        // cache a power loss could wipe out. Real Kafka batches many
        // records per flush for performance; we flush every record for
        // now to keep the durability guarantee simple and obvious.
        file.getFD().sync();

        return offset;
    }

    /** Reads back the record whose length-prefix starts at this offset. */
    public synchronized byte[] read(long offset) throws IOException {
        file.seek(offset);

        byte[] lengthBytes = new byte[4];
        file.readFully(lengthBytes);
        int length = ByteBuffer.wrap(lengthBytes).getInt();

        byte[] payload = new byte[length];
        file.readFully(payload);

        return payload;
    }

    /** The offset one past the last record written. */
    public synchronized long endOffset() throws IOException {
        return file.length();
    }

    @Override
    public void close() throws IOException {
        file.close();
    }
}
