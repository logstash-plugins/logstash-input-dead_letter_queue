package org.logstash.input;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.logstash.common.io.DeadLetterQueueReader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

interface SinceDB {

    Path getPath();

    void flush();

    Path getCurrentSegment();

    long getOffset();

    boolean isAssigned();


    /**
     * Load SinceDB from given path.
     *
     * @param sinceDbPath of sinceDB
     * @return new instance of SinceDB. This instance may be unassigned if the path does not exist, or has no entries written.
     *
     */
    static SinceDB fromPath(Path sinceDbPath) throws IOException {
        if (sinceDbPath == null) {
            throw new IllegalArgumentException("sinceDbPath can't be null");
        }

        if (!sinceDbPath.toFile().exists()) {
            return new UnassignedDB(sinceDbPath);
        }

        byte[] bytes = Files.readAllBytes(sinceDbPath);
        if (bytes.length == 0) {
            return new UnassignedDB(sinceDbPath);
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        verifyVersion(buffer);
        Path segmentPath = decodePath(buffer);
        long offset = buffer.getLong();

        return new AssignedDB(sinceDbPath, segmentPath, offset);
    }

    /*private*/ static Path decodePath(ByteBuffer buffer) {
        int segmentPathStringLength = buffer.getInt();
        byte[] segmentPathBytes = new byte[segmentPathStringLength];
        buffer.get(segmentPathBytes);
        return Paths.get(new String(segmentPathBytes));
    }

    /*private*/ static void verifyVersion(ByteBuffer buffer) {
        char version = buffer.getChar();
        if (DeadLetterQueueInputPlugin.VERSION != version) {
            throw new RuntimeException("Sincedb version:" + version + " does not match: " + DeadLetterQueueInputPlugin.VERSION);
        }
    }

    /**
     * Create new SinceDB instance from the position of the reader.
     *
     * @param oldSinceDb of SinceDB to update
     * @param reader the reader that needs to be queried.
     * @return new instance of SinceDB containing the new position.
     */
    static SinceDB getUpdated(SinceDB oldSinceDb, DeadLetterQueueReader reader) {
        return new AssignedDB(oldSinceDb.getPath(), reader.getCurrentSegment(), reader.getCurrentPosition());
    }

    /**
     * Attempts to update the SinceDB. This operation may throw a NullPointerException
     * if DeadLetterQueueReader.currentReader is null, which can occur if
     * DeadLetterQueueReader.segments has never had an available segment.
     * The exception is caught and ignored.
     * @param oldSinceDb
     * @param reader
     * @return new instance of SinceDB or UnassignedDB
     */
    static SinceDB mayUpdate(SinceDB oldSinceDb, DeadLetterQueueReader reader) {
        try {
            return getUpdated(oldSinceDb, reader);
        } catch (NullPointerException e) {
            return new UnassignedDB(oldSinceDb.getPath());
        }
    }

    final class UnassignedDB implements SinceDB {
        private final Path sinceDbPath;

        UnassignedDB(Path sinceDbPath) {
            this.sinceDbPath = sinceDbPath;
        }

        @Override
        public Path getPath() {
            return sinceDbPath;
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


    final class AssignedDB implements SinceDB {
        private static final Logger logger = LogManager.getLogger(AssignedDB.class);

        private final Path sinceDbPath;
        private final Path currentSegment;
        private final long offset;

        AssignedDB(Path sinceDbPath, Path currentSegment, long offset) {
            this.sinceDbPath = sinceDbPath;
            this.currentSegment = currentSegment;
            this.offset = offset;
        }

        @Override
        public Path getPath() {
            return sinceDbPath;
        }

        @Override
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

        @Override
        public Path getCurrentSegment() {
            return currentSegment;
        }

        @Override
        public long getOffset() {
            return offset;
        }

        @Override
        public boolean isAssigned() {
            return true;
        }
    }
}
