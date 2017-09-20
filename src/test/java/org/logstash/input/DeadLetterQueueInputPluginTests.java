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

import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.logstash.DLQEntry;
import org.logstash.Event;
import org.logstash.Timestamp;
import org.logstash.common.io.DeadLetterQueueWriter;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

import static junit.framework.TestCase.assertEquals;

public class DeadLetterQueueInputPluginTests {

    private Path dir;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void setUp() throws Exception {
        dir = temporaryFolder.newFolder().toPath();
    }

    @Test
    public void test() throws Exception {
        DeadLetterQueueWriter queueWriter = new DeadLetterQueueWriter(dir, 100000000, 10000000);
        DLQEntry entry = new DLQEntry(new Event(), "test", "test", "test");
        for (int i = 0; i < 10000; i++) {
            queueWriter.writeEntry(entry);
        }

        Path since = temporaryFolder.newFile(".sincdb").toPath();
        DeadLetterQueueInputPlugin plugin = new DeadLetterQueueInputPlugin(dir, true, since, null);

        final AtomicInteger count = new AtomicInteger();
        Thread pluginThread = new Thread(() -> {
            try {
                plugin.register();
                plugin.run((e) -> {count.incrementAndGet();});
            } catch (Exception e) {
                // do nothing
            }
        });
        pluginThread.start();
        Thread.sleep(15000);
        assertEquals(10000, count.get());
        queueWriter.writeEntry(entry);
        Thread.sleep(200);
        assertEquals(10001, count.get());
        pluginThread.interrupt();
        pluginThread.join();
        plugin.close();

        queueWriter.writeEntry(entry);
        queueWriter.writeEntry(entry);

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
        Thread.sleep(200);
        pluginThread.interrupt();
        pluginThread.join();
        secondPlugin.close();
        assertEquals(10003, count.get());
    }

    @Test
    public void testTimestamp() throws Exception {
        DeadLetterQueueWriter queueWriter = new DeadLetterQueueWriter(dir, 100000, 10000000);
        long epoch = 1490659200000L;
        String targetDateString = "";
        for (int i = 0; i < 10000; i++) {
            DLQEntry entry = new DLQEntry(new Event(), "test", "test", "test", new Timestamp(epoch));
            queueWriter.writeEntry(entry);
            epoch += 1000;
            if (i == 800) {
                targetDateString = entry.getEntryTime().toString();
            }
        }
        DeadLetterQueueInputPlugin plugin = new DeadLetterQueueInputPlugin(dir, false, null, new Timestamp(targetDateString));
        plugin.register();
    }
}
