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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;


public class DeadLetterQueueInputPlugin {
    private static final Logger logger = LogManager.getLogger(DeadLetterQueueInputPlugin.class);

    private final static char VERSION = '1';
    private final DeadLetterQueueReader queueReader;
    private final boolean commitOffsets;
    private final Path sinceDbPath;
    private final AtomicBoolean open;
    private final Timestamp targetTimestamp;

    public DeadLetterQueueInputPlugin(Path path, boolean commitOffsets, Path sinceDbPath, Timestamp targetTimestamp) throws Exception {
        this.queueReader = new DeadLetterQueueReader(path);
        this.commitOffsets = commitOffsets;
        this.open = new AtomicBoolean(true);
        this.sinceDbPath = sinceDbPath;
        this.targetTimestamp = targetTimestamp;
    }

    public DeadLetterQueueReader getQueueReader() {
        return queueReader;
    }

    public void register() throws IOException {
        if (sinceDbPath != null && Files.exists(sinceDbPath) && targetTimestamp == null) {
            byte[] bytes = Files.readAllBytes(sinceDbPath);
            if (bytes.length == 0) {
                return;
            }
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            char version = buffer.getChar();
            if (VERSION != version) {
                throw new RuntimeException("Sincdb version:" + version + " does not match: " +  VERSION);
            }
            int segmentPathStringLength = buffer.getInt();
            byte[] segmentPathBytes = new byte[segmentPathStringLength];
            buffer.get(segmentPathBytes);
            long offset = buffer.getLong();
            queueReader.setCurrentReaderAndPosition(Paths.get(new String(segmentPathBytes)), offset);
        } else if (targetTimestamp != null) {
            queueReader.seekToNextEvent(targetTimestamp);
        }
    }

    public void run(Consumer<Queueable> queueConsumer) throws Exception {
        while (open.get()) {
            DLQEntry entry = queueReader.pollEntry(100);
            if (entry != null) {
                queueConsumer.accept(entry);
            }
        }
    }

    private void writeOffsets(Path segment, long offset) throws IOException {
        logger.info("writing offsets");
        String path = segment.toAbsolutePath().toString();
        ByteBuffer buffer = ByteBuffer.allocate(path.length() + 1 + 64);
        buffer.putChar(VERSION);
        buffer.putInt(path.length());
        buffer.put(path.getBytes());
        buffer.putLong(offset);
        Files.write(sinceDbPath, buffer.array());
    }

    public void close() throws IOException {
        logger.warn("closing dead letter queue input plugin");
        if (commitOffsets) {
            writeOffsets(queueReader.getCurrentSegment(), queueReader.getCurrentPosition());
        }
        queueReader.close();
        open.set(false);
    }
}
