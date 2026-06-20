package com.kafkaclone;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;


public class CommitLog implements AutoCloseable {

    private final RandomAccessFile file;

    public CommitLog(String path) throws IOException {
        this.file = new RandomAccessFile(path, "rw");
        // Start writing new records at the end of whatever's already
        // there, so restarting the process doesn't erase history.
        this.file.seek(this.file.length());
    }

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

    public synchronized byte[] read(long offset) throws IOException {
        file.seek(offset);

        byte[] lengthBytes = new byte[4];
        file.readFully(lengthBytes);
        int length = ByteBuffer.wrap(lengthBytes).getInt();

        byte[] payload = new byte[length];
        file.readFully(payload);

        return payload;
    }

    public synchronized long endOffset() throws IOException {
        return file.length();
    }

    @Override
    public void close() throws IOException {
        file.close();
    }
}
