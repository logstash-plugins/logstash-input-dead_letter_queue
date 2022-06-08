package org.logstash.input;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.logstash.common.io.DeadLetterQueueReader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

class SinceDB {

    final static class UnassignedSinceDB extends SinceDB {

        private UnassignedSinceDB(Path sinceDbPath) {
            super(sinceDbPath, Paths.get(System.getProperty("user.home")), 0);
        }

        @Override
        public boolean isAssigned() {
            return false;
        }

        @Override
        public void flush() {
            // unassigned SinceDB hasn't anything to flush
        }

        @Override
        public Path getCurrentSegment() {
            throw new IllegalStateException("Unassigned SinceDB doesn't have currentSegment");
        }

        @Override
        public long getOffset() {
            throw new IllegalStateException("Unassigned SinceDB doesn't have offset");
        }
    }

    private static final Logger logger = LogManager.getLogger(SinceDB.class);

    private final Path sinceDbPath;
    private final Path currentSegment;
    private final long offset;

    private SinceDB(Path sinceDbPath, Path currentSegment, long offset) {
        this.sinceDbPath = sinceDbPath;
        this.currentSegment = currentSegment;
        this.offset = offset;
    }

    static SinceDB load(Path sinceDbPath) throws IOException {
        byte[] bytes = Files.readAllBytes(sinceDbPath);
        if (bytes.length == 0) {
            return createUnassigned(sinceDbPath);
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        verifyVersion(buffer);
        Path segmentPath = decodePath(buffer);
        long offset = buffer.getLong();

        return new SinceDB(sinceDbPath, segmentPath, offset);
    }

    private static Path decodePath(ByteBuffer buffer) {
        int segmentPathStringLength = buffer.getInt();
        byte[] segmentPathBytes = new byte[segmentPathStringLength];
        buffer.get(segmentPathBytes);
        return Paths.get(new String(segmentPathBytes));
    }

    private static void verifyVersion(ByteBuffer buffer) {
        char version = buffer.getChar();
        if (DeadLetterQueueInputPlugin.VERSION != version) {
            throw new RuntimeException("Sincedb version:" + version + " does not match: " + DeadLetterQueueInputPlugin.VERSION);
        }
    }

    static SinceDB createUnassigned(Path sinceDbPath) {
        return new UnassignedSinceDB(sinceDbPath);
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

    public SinceDB updatePosition(DeadLetterQueueReader reader) {
        return new SinceDB(this.sinceDbPath, reader.getCurrentSegment(), reader.getCurrentPosition());
    }

    public boolean isAssigned() {
        return true;
    }
}
