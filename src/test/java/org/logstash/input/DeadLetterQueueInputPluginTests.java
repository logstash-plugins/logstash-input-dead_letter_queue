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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.logstash.DLQEntry;
import org.logstash.Event;
import org.logstash.Timestamp;
import org.logstash.common.io.DeadLetterQueueWriter;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class DeadLetterQueueInputPluginTests {

    private Path dir;
    private final DeadLetterQueueInputPlugin.UpdateConsumedMetrics metricsSink = new DeadLetterQueueInputPlugin.UpdateConsumedMetrics() {
        @Override
        public void segmentsDeleted(int segments, long events) {

        }
    };

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void setUp() throws Exception {
        dir = temporaryFolder.newFolder().toPath();
    }

    // DeadLetterQueueWriter.newBuilder gained a fifth argument (flushCheckInterval) in
    // Logstash 9.x. Resolve and invoke the available overload reflectively so this test
    // compiles and runs against both the 8.x (4-arg) and 9.x (5-arg) logstash-core.
    private static DeadLetterQueueWriter newWriter(Path dir, long maxSegmentSize, long maxQueueSize,
                                                   Duration flushInterval) throws Exception {
        for (Method m : DeadLetterQueueWriter.class.getMethods()) {
            if (!m.getName().equals("newBuilder") || !Modifier.isStatic(m.getModifiers())) {
                continue;
            }
            final Object builder;
            switch (m.getParameterCount()) {
                case 4:
                    builder = m.invoke(null, dir, maxSegmentSize, maxQueueSize, flushInterval);
                    break;
                case 5:
                    builder = m.invoke(null, dir, maxSegmentSize, maxQueueSize, flushInterval, flushInterval);
                    break;
                default:
                    continue;
            }
            return (DeadLetterQueueWriter) builder.getClass().getMethod("build").invoke(builder);
        }
        throw new IllegalStateException("No compatible DeadLetterQueueWriter.newBuilder overload found");
    }

    // Poll until the reader has emitted at least `target` events, or the timeout
    // elapses. Logstash 9.x (especially 9.previous) flushes/reads the DLQ on a
    // slower cadence than the original fixed sleeps assumed, which made those
    // sleeps flaky; polling keeps the test fast when quick and tolerant when slow.
    private static void awaitCount(AtomicInteger count, int target, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (count.get() < target && System.currentTimeMillis() < deadline) {
            Thread.sleep(100);
        }
    }

    @Test
    public void testHappyPath() throws Exception {
        DeadLetterQueueWriter queueWriter = newWriter(dir, 10_000_000, 10_000_000, Duration.ofMillis(100));
        for (int i = 0; i < 10_000; i++) {
            writeEntry(queueWriter, new DLQEntry(new Event(), "test-type", "test-id", "test_" + i));
        }

        Path since = temporaryFolder.newFile(".sincedb").toPath();
        DeadLetterQueueInputPlugin plugin = new DeadLetterQueueInputPlugin(dir, true, since, null, false, metricsSink);

        final AtomicInteger count = new AtomicInteger();
        Thread pluginThread = new Thread(() -> {
            try {
                plugin.register();
                plugin.run((e) -> { count.incrementAndGet(); });
            } catch (Exception e) {
                // do nothing
            }
        });
        pluginThread.start();

        DLQEntry entry = new DLQEntry(new Event(), "test-type", "test-id", "test_shared");

        awaitCount(count, 10000, 30000);
        assertEquals(10000, count.get());
        writeEntry(queueWriter, entry);
        awaitCount(count, 10001, 15000);
        assertEquals(10001, count.get());
        pluginThread.interrupt();
        pluginThread.join();
        plugin.close();

        writeEntry(queueWriter, entry);
        writeEntry(queueWriter, entry);

        DeadLetterQueueInputPlugin secondPlugin = new DeadLetterQueueInputPlugin(dir, true, since, null, false, metricsSink);

        pluginThread = new Thread(() -> {
            try {
                secondPlugin.register();
                secondPlugin.run((e) -> {count.incrementAndGet();});
            } catch (Exception e) {
                // do nothing
            }
        });
        pluginThread.start();
        awaitCount(count, 10003, 15000);
        pluginThread.interrupt();
        pluginThread.join();
        secondPlugin.close();
        assertEquals(10003, count.get());
    }

    @Test
    public void testTimestamp() throws Exception {
        DeadLetterQueueWriter queueWriter = newWriter(dir, 100_000, 10_000_000, Duration.ofMillis(1000));
        long epoch = 1490659200000L;
        String targetDateString = "";
        for (int i = 0; i < 10000; i++) {
            DLQEntry entry = new DLQEntry(new Event(), "test", "test", "test", new Timestamp(epoch));
            writeEntry(queueWriter, entry);
            epoch += 1000;
            if (i == 800) {
                targetDateString = entry.getEntryTime().toString();
            }
        }
        Path since = temporaryFolder.newFile(".sincedb").toPath();
        DeadLetterQueueInputPlugin plugin = new DeadLetterQueueInputPlugin(dir, false, since, new Timestamp(targetDateString), false, metricsSink);
        plugin.register();
    }

    @Test
    public void testEmptyDlqShouldNotThrowsNPEWhenClose() throws Exception {
        // Create SinceDB file
        Path garbageSegment = temporaryFolder.newFile("1.log").toPath();
        Path sincePath = temporaryFolder.newFile(".sincedb").toPath();
        SinceDB sinceDB = new SinceDB.AssignedDB(sincePath, garbageSegment, 0);
        sinceDB.flush();
        // Delete segment in DLQ folder
        Files.delete(garbageSegment);

        DeadLetterQueueInputPlugin plugin = new DeadLetterQueueInputPlugin(dir, true, sincePath, null, false, metricsSink);

        plugin.register();
        plugin.close();
    }

    @Test
    public void testClosingEmptyDlq() throws Exception {
        // Plugin does nothing and does not crash
        Path since = temporaryFolder.newFile(".sincedb").toPath();
        DeadLetterQueueInputPlugin plugin = new DeadLetterQueueInputPlugin(dir, true, since, null, false, metricsSink);

        plugin.register();
        plugin.close();
    }

    @Test
    public void testNonExistentQueuePath() throws Exception {
        Path since = temporaryFolder.newFile(".sincedb").toPath();
        Path queuePath = Paths.get(temporaryFolder.toString(), "non-existent");

        int times = 0;
        while (times++ < 1000) {
            try {
                DeadLetterQueueInputPlugin plugin = new DeadLetterQueueInputPlugin(queuePath, true, since, null, false, metricsSink);
                plugin.register();
                plugin.run((entry) -> { assertNotNull(entry); });
            } catch (NoSuchFileException e) {
                // expected
            } catch (IOException e) {
                assertNotEquals("User limit of inotify instances reached or too many open files", e.getMessage());
                throw e;
            }
        }
    }

    private static void writeEntry(DeadLetterQueueWriter writer, DLQEntry entry) throws IOException {
        writer.writeEntry(entry.getEvent(), entry.getPluginType(), entry.getPluginId(), entry.getReason());
    }

}
