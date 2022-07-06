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
import org.logstash.common.io.SegmentListener;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;


public class DeadLetterQueueInputPlugin implements SegmentListener {
    private static final Logger logger = LogManager.getLogger(DeadLetterQueueInputPlugin.class);

    private static final int MAX_FLUSH_READS = 100;

    final static char VERSION = '1';
    private final Path queuePath;
    private final boolean commitOffsets;
    private final boolean cleanConsumed;
    private final Path sinceDbPath;
    private final AtomicBoolean open, readerHasState;
    private final Timestamp targetTimestamp;

    private volatile DeadLetterQueueReader queueReader;
    private SinceDB sinceDb;

    public DeadLetterQueueInputPlugin(Path path, boolean commitOffsets, Path sinceDbPath, Timestamp targetTimestamp,
                                      boolean cleanConsumed) throws IOException {
        this.queuePath = path;
        this.commitOffsets = commitOffsets;
        this.cleanConsumed = cleanConsumed;
        this.open = new AtomicBoolean(true);
        this.sinceDbPath = sinceDbPath;
        this.targetTimestamp = targetTimestamp;
        this.readerHasState = new AtomicBoolean(false);
        this.sinceDb = SinceDB.fromPath(sinceDbPath);
    }

    private synchronized DeadLetterQueueReader lazyInitQueueReader() throws IOException {
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
            if (cleanConsumed) {
                // cleanConsumed is true only for Logstash >= 8.4.0 which provides this constructor,
                // else fallback to the old constructor.
                this.queueReader = new DeadLetterQueueReader(queuePath, cleanConsumed, this);
            } else {
                this.queueReader = new DeadLetterQueueReader(queuePath);
            }
            setInitialReaderState(queueReader);
        }
        return queueReader;
    }

    @Override
    public void segmentCompleted() {
        sinceDb = SinceDB.getUpdated(sinceDb, queueReader);
        sinceDb.flush();
    }

    @Override
    public void segmentsDeleted(int segments, long events) {

    }

    public void register() throws IOException {
        if (queuePath.toFile().isDirectory()) {
            lazyInitQueueReader(); // NOTE: reading sincedb 'early' for backwards compatibility, should be fine to remove
        }
    }

    private void setInitialReaderState(final DeadLetterQueueReader queueReader) throws IOException {
        if (sinceDbPath != null && Files.exists(sinceDbPath) && targetTimestamp == null) {
            sinceDb = SinceDB.fromPath(sinceDbPath);
            if (!sinceDb.isAssigned()) {
                return;
            }

            queueReader.setCurrentReaderAndPosition(sinceDb.getCurrentSegment(), sinceDb.getOffset());
            readerHasState.set(true);
        } else if (targetTimestamp != null) {
            queueReader.seekToNextEvent(targetTimestamp);
            readerHasState.set(false);
        }
    }

    public void run(Consumer<Queueable> queueConsumer) throws IOException, InterruptedException {
        final DeadLetterQueueReader queueReader = lazyInitQueueReader();
        int readsCounter = 0;

        while (open.get()) {
            DLQEntry entry = queueReader.pollEntry(100);
            if (entry != null) {
                readerHasState.set(true);
                queueConsumer.accept(entry);
                if (cleanConsumed) {
                    readsCounter++;
                    if (readsCounter >= MAX_FLUSH_READS) {
                        queueReader.markForDelete();
                        readsCounter = 0;
                    }
                }
            }
        }
    }

    public void close() {
        open.set(false);

        final DeadLetterQueueReader queueReader = this.queueReader;
        if (queueReader != null && commitOffsets && readerHasState.get()) {
            logger.debug("retrieving current DLQ segment and position");
            sinceDb = SinceDB.getUpdated(sinceDb, queueReader);
        }

        try {
            logger.debug("closing DLQ reader");
            if (queueReader != null) {
                queueReader.close();
            }
        } catch (Exception e) {
            logger.warn("error closing DLQ reader", e);
        } finally {
            sinceDb.flush();
        }
    }
}
