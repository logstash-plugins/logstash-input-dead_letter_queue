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
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class DeadLetterQueueInputPluginTests {

    private Path dir;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void setUp() throws Exception {
        dir = temporaryFolder.newFolder().toPath();
    }

    @Test
    public void testHappyPath() throws Exception {
        DeadLetterQueueWriter queueWriter = new DeadLetterQueueWriter(dir, 10_000_000, 10_000_000, Duration.ofMillis(100));
        for (int i = 0; i < 10_000; i++) {
            writeEntry(queueWriter, new DLQEntry(new Event(), "test-type", "test-id", "test_" + i));
        }

        Path since = temporaryFolder.newFile(".sincedb").toPath();
        DeadLetterQueueInputPlugin plugin = new DeadLetterQueueInputPlugin(dir, true, since, null);

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

        Thread.sleep(15000);
        assertEquals(10000, count.get());
        writeEntry(queueWriter, entry);
        Thread.sleep(1500); // flush interval 1s
        assertEquals(10001, count.get());
        pluginThread.interrupt();
        pluginThread.join();
        plugin.close();

        writeEntry(queueWriter, entry);
        writeEntry(queueWriter, entry);

        DeadLetterQueueInputPlugin secondPlugin = new DeadLetterQueueInputPlugin(dir, true, since, null);

        pluginThread = new Thread(() -> {
            try {
                secondPlugin.register();
                secondPlugin.run((e) -> {count.incrementAndGet();});
            } catch (Exception e) {
                // do nothing
            }
        });
        pluginThread.start();
        Thread.sleep(1500); // flush interval 1s
        pluginThread.interrupt();
        pluginThread.join();
        secondPlugin.close();
        assertEquals(10003, count.get());
    }

    @Test
    public void testTimestamp() throws Exception {
        DeadLetterQueueWriter queueWriter = new DeadLetterQueueWriter(dir, 100_000, 10_000_000, Duration.ofMillis(1000));
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
        DeadLetterQueueInputPlugin plugin = new DeadLetterQueueInputPlugin(dir, false, null, new Timestamp(targetDateString));
        plugin.register();
    }

    @Test
    public void testClosingEmptyDlq() throws Exception {
        // Plugin does nothing and does not crash
        Path since = temporaryFolder.newFile(".sincedb").toPath();
        DeadLetterQueueInputPlugin plugin = new DeadLetterQueueInputPlugin(dir, true, since, null);

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
                DeadLetterQueueInputPlugin plugin = new DeadLetterQueueInputPlugin(queuePath, true, since, null);
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
