package com.kafkaclone;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * PHASE 3 -- a single named consumer's durable "bookmark" for one topic,
 * persisted to disk so a server restart doesn't forget it.
 */
public class ConsumerOffsetStore {

    private final Path file;

    public ConsumerOffsetStore(String dataDirectory, String topic, String consumerName) {
        this.file = Path.of(dataDirectory, topic + "." + consumerName + ".offset");
    }

    public long load() throws IOException {
        if (!Files.exists(file)) {
            return 0L; // brand-new consumer: start from the beginning of the log
        }
        return Long.parseLong(Files.readString(file).trim());
    }

    /**
     * Write-temp-file-then-rename: we never touch the real offset file
     * directly, so a crash mid-save can never leave it half-written --
     * rename() is atomic on essentially every filesystem.
     */
    public void save(long offset) throws IOException {
        Path tmp = Path.of(file.toString() + ".tmp");
        Files.writeString(tmp, Long.toString(offset), StandardCharsets.UTF_8);
        Files.move(tmp, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }
}
