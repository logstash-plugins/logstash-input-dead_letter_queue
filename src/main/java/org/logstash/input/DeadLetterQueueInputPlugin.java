/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.logstash.input;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.logstash.DLQEntry;
import org.logstash.Timestamp;
import org.logstash.ackedqueue.Queueable;
import org.logstash.common.io.DeadLetterQueueReader;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;


public class DeadLetterQueueInputPlugin {
    private static final Logger logger = LogManager.getLogger(DeadLetterQueueInputPlugin.class);

    private final static char VERSION = '1';
    private final Path queuePath;
    private final boolean commitOffsets;
    private final Path sinceDbPath;
    private final AtomicBoolean open, readerHasState;
    private final Timestamp targetTimestamp;

    private volatile DeadLetterQueueReader queueReader;

    public DeadLetterQueueInputPlugin(Path path, boolean commitOffsets, Path sinceDbPath, Timestamp targetTimestamp) {
        this.queuePath = path;
        this.commitOffsets = commitOffsets;
        this.open = new AtomicBoolean(true);
        this.sinceDbPath = sinceDbPath;
        this.targetTimestamp = targetTimestamp;
        this.readerHasState = new AtomicBoolean(false);
    }

    private synchronized DeadLetterQueueReader getQueueReader() throws IOException {
        if (queueReader == null) {
            final File queueDir = queuePath.toFile();
            // NOTE: avoid creating DLQReader if these fail so that on plugin restarts the inotify limit is not decremented
            if (!queueDir.exists()) {
                logger.warn("DLQ sub-path {} does not exist", queuePath);
                throw new NoSuchFileException("DLQ sub-path " + queuePath + " does not exist");
            }
            if (!queueDir.isDirectory()) {
                logger.warn("DLQ sub-path {} is not a directory", queuePath);
                throw new NotDirectoryException("DLQ sub-path " + queuePath + " is not a directory");
            }
            this.queueReader = new DeadLetterQueueReader(queuePath);
            setInitialReaderState(queueReader);
        }
        return queueReader;
    }

    public void register() throws IOException {
        if (queuePath.toFile().isDirectory()) {
            getQueueReader(); // NOTE: reading sincedb 'early' for backwards compatibility, should be fine to remove
        }
    }

    private void setInitialReaderState(final DeadLetterQueueReader queueReader) throws IOException {
        if (sinceDbPath != null && Files.exists(sinceDbPath) && targetTimestamp == null) {
            byte[] bytes = Files.readAllBytes(sinceDbPath);
            if (bytes.length == 0) {
                return;
            }
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            char version = buffer.getChar();
            if (VERSION != version) {
                throw new RuntimeException("Sincedb version:" + version + " does not match: " + VERSION);
            }
            int segmentPathStringLength = buffer.getInt();
            byte[] segmentPathBytes = new byte[segmentPathStringLength];
            buffer.get(segmentPathBytes);
            long offset = buffer.getLong();
            queueReader.setCurrentReaderAndPosition(Paths.get(new String(segmentPathBytes)), offset);
            readerHasState.set(true);
        } else if (targetTimestamp != null) {
            queueReader.seekToNextEvent(targetTimestamp);
            readerHasState.set(false);
        }
    }

    public void run(Consumer<Queueable> queueConsumer) throws IOException, InterruptedException {
        final DeadLetterQueueReader queueReader = getQueueReader();

        while (open.get()) {
            DLQEntry entry = queueReader.pollEntry(100);
            if (entry != null) {
                readerHasState.set(true);
                queueConsumer.accept(entry);
            }
        }
    }

    private void writeOffsets(final Path segment, final long offset) throws IOException {
        logger.debug("writing DLQ offset state: {} (position: {})", segment, offset);
        String path = segment.toAbsolutePath().toString();
        ByteBuffer buffer = ByteBuffer.allocate(path.length() + 1 + 64);
        buffer.putChar(VERSION);
        buffer.putInt(path.length());
        buffer.put(path.getBytes());
        buffer.putLong(offset);
        Files.write(sinceDbPath, buffer.array());
    }

    public void close() throws IOException {
        open.set(false);

        CurrentSegmentAndPosition state = null;
        if (commitOffsets && readerHasState.get()) {
            logger.debug("retrieving current DLQ segment and position");
            try {
                final DeadLetterQueueReader queueReader = getQueueReader();
                state = new CurrentSegmentAndPosition(queueReader.getCurrentSegment(), queueReader.getCurrentPosition());
            } catch (Exception e) {
                logger.error("failed to retrieve current DLQ segment and position", e);
            }
        }

        try {
            logger.debug("closing DLQ reader");
            final DeadLetterQueueReader queueReader = this.queueReader;
            if (queueReader != null) queueReader.close();
        } catch (Exception e) {
            logger.warn("error closing DLQ reader", e);
        } finally {
            if (state != null) writeOffsets(state.segmentPath, state.position);
        }
    }

    private static class CurrentSegmentAndPosition {
        final Path segmentPath;
        final long position;

        CurrentSegmentAndPosition(Path segmentPath, long position) {
            this.segmentPath = segmentPath;
            this.position = position;
        }
    }

}
