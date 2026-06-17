# kafka-clone-java -- Phases 1 through 5

A from-scratch Kafka-style broker built up phase by phase, in plain
Java + Gradle. This snapshot includes:

- **Phase 1** -- a TCP server that runs forever, accepting connection
  after connection, parsing simple text commands.
- **Phase 2** -- `CommitLog.java`: a real append-only log file per
  topic, with length-prefixed records and fsync on every write.
- **Phase 3** -- `ConsumerOffsetStore.java`: durable, named-consumer
  "bookmarks" persisted to disk (crash-safe via write-temp-then-rename),
  plus at-least-once delivery semantics (a message is redelivered until
  explicitly acknowledged).
- **Phase 4** -- `Framing.java`: length-prefixed binary framing over the
  socket, replacing the old line-based protocol, so message content can
  contain anything, including newlines.
- **Phase 5** -- the server now hands every connection off to its own
  virtual thread (Java 21), so one slow client can't block any other
  client. `Broker.java`'s public methods are `synchronized` to keep this
  safe -- a deliberately coarse, whole-broker lock that a later phase
  (partitions) will shard.

## Project layout

```
kafka-clone-java/
  build.gradle
  settings.gradle
  src/main/java/com/kafkaclone/
    Server.java              -- network loop + command dispatch
    Client.java               -- interactive test client
    Broker.java                -- ties topics, logs, and offsets together
    CommitLog.java              -- the append-only log file
    ConsumerOffsetStore.java     -- durable per-consumer bookmark
    Framing.java                  -- length-prefixed read/write helpers
```

## Opening the project

**IntelliJ IDEA**: File -> Open -> select this folder. IntelliJ will
detect `build.gradle` and offer to import it as a Gradle project --
accept that; it sets up the Gradle wrapper automatically (needs internet
the first time, to fetch Gradle itself).

**VS Code**: open the folder with the "Extension Pack for Java" and
"Gradle for Java" extensions installed; same import flow.

**Gradle CLI**: run `gradle wrapper` once inside this folder to generate
`gradlew`, then `./gradlew run` going forward.

## Running it

Run the server and client as **separate processes**, not through
`./gradlew run` for the server specifically -- see the note below on why.

1. **Server**: open `Server.java`, right-click the `main` method, choose
   Run. You should see `Server listening on port 9092...` and it stays
   running indefinitely (that's correct -- it's a server).

   A note on a confusing-looking error: if you ever launch the server via
   `./gradlew run` and later stop it (Ctrl+C, IDE Stop button, closing
   the terminal), Gradle will print something like `BUILD FAILED` /
   `Build cancelled while executing task ':...Server.main()'`. That's
   not a crash -- it's just Gradle reporting that a task which was never
   going to finish on its own got interrupted. Running directly via the
   IDE's "Run" on `Server.java` avoids this noise entirely.

2. **Client**: open `Client.java`, Run. Type commands one at a time,
   pressing Enter after each:

   - `PING` -> `PONG`
   - `PUBLISH orders hello world` -> `OK 0` (the offset it was written at)
   - `CONSUME orders alice` -> `MESSAGE NEW hello world`
   - `CONSUME orders alice` again, without acking -> the *same* message,
     now tagged `MESSAGE REDELIVERED hello world` -- this is at-least-once
     delivery in action.
   - `ACK orders alice` -> `OK`
   - `CONSUME orders alice` again -> `EOF` (nothing new since the only
     message published has now been acknowledged)
   - `QUIT` -> `BYE`, closes the connection.

3. **Concurrency check**: with the server still running from step 1,
   start a *second* `Client` while the first one is still connected and
   mid-conversation. It should connect and respond instantly -- neither
   client blocks the other. (Before Phase 5, the second client would have
   hung until the first sent `QUIT`.)

4. **Durability check**: after publishing and partially consuming some
   messages, stop the server completely and start it again. Run
   `CONSUME` with the same consumer name you used before -- it resumes
   from exactly where it left off, because the committed offset was
   persisted to disk the whole time, not just held in memory.

## What's on disk

Everything lives under a `data/` folder created next to wherever you run
the server from:

- `data/<topic>.log` -- the append-only log of every message published
  to that topic.
- `data/<topic>.<consumerName>.offset` -- that consumer's durable
  bookmark into that topic's log.

Delete the `data/` folder to reset everything to a clean slate.

## Known limitations at this point (intentional -- coming in later phases)

- One global lock across the entire broker: unrelated topics/consumers
  still wait on each other. Fixed by partitions.
- No partitions: a topic is exactly one log file, so there's no way to
  parallelize work across multiple consumer processes sharing a topic.
- No replication: one server, one disk -- a lost disk means lost data.
- Commands are still parsed as plain text inside each frame (only the
  framing/boundary mechanism is binary, not the command encoding itself).
