package org.logstash.input;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.logstash.common.io.DeadLetterQueueReader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

final class SinceDB {
    private static final Logger logger = LogManager.getLogger(SinceDB.class);

    private final Path sinceDbPath;
    private Path currentSegment;
    private long offset;

    private SinceDB(Path sinceDbPath, Path currentSegment, long offset) {
        this.sinceDbPath = sinceDbPath;
        this.currentSegment = currentSegment;
        this.offset = offset;
    }

    static Optional<SinceDB> load(Path sinceDbPath) throws IOException {
        byte[] bytes = Files.readAllBytes(sinceDbPath);
        if (bytes.length == 0) {
            return Optional.empty();
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        verifyVersion(buffer);
        Path segmentPath = decodePath(buffer);
        long offset = buffer.getLong();

        return Optional.of(new SinceDB(sinceDbPath, segmentPath, offset));
    }

    private static Path decodePath(ByteBuffer buffer) {
        int segmentPathStringLength = buffer.getInt();
        byte[] segmentPathBytes = new byte[segmentPathStringLength];
        buffer.get(segmentPathBytes);
        Path segmentPath = Paths.get(new String(segmentPathBytes));
        return segmentPath;
    }

    private static void verifyVersion(ByteBuffer buffer) {
        char version = buffer.getChar();
        if (DeadLetterQueueInputPlugin.VERSION != version) {
            throw new RuntimeException("Sincedb version:" + version + " does not match: " + DeadLetterQueueInputPlugin.VERSION);
        }
    }

    static Optional<SinceDB> createEmpty(Path sinceDbPath) {
        return Optional.of(new SinceDB(sinceDbPath, Paths.get(System.getProperty("user.home")), 0));
    }

    public void flush() {
        if (currentSegment == null) {
            return;
        }
        logger.debug("Flushing DLQ last read position");
        String path = currentSegment.toAbsolutePath().toString();
        ByteBuffer buffer = ByteBuffer.allocate(path.length() + 1 + 64);
        buffer.putChar(DeadLetterQueueInputPlugin.VERSION);
        buffer.putInt(path.length());
        buffer.put(path.getBytes());
        buffer.putLong(offset);
        try {
            Files.write(sinceDbPath, buffer.array());
        } catch (IOException e) {
            logger.error("failed to write DLQ offset state to " + sinceDbPath, e);
        }
    }


    public Path getCurrentSegment() {
        return currentSegment;
    }

    public long getOffset() {
        return offset;
    }

    public void updatePosition(DeadLetterQueueReader reader) {
        updatePosition(reader.getCurrentSegment(), reader.getCurrentPosition());
    }

    private void updatePosition(Path segment, long offset) {
        this.currentSegment = segment;
        this.offset = offset;
    }
}
