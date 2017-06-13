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

import java.io.IOException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.logstash.DLQEntry;
import org.logstash.Event;
import org.logstash.Timestamp;

import org.logstash.common.io.DeadLetterQueueWriter;

import java.nio.file.Path;
import java.util.concurrent.atomic.LongAdder;

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
    public void testConsumeTwiceNoOffsetsWithDate() throws Exception {
        DeadLetterQueueWriter queueWriter = null;
        try {
            queueWriter = new DeadLetterQueueWriter(dir, 100000000, 10000000);
            Timestamp cutoffTimestamp = writeEventsWithCutoff(queueWriter, 1000, 800);

            try(DeadLetterQueueInputPlugin plugin = new DeadLetterQueueInputPlugin(dir, false, null, cutoffTimestamp)) {
                assertMessagesReceived(plugin, 200);
            }
            writeEvents(queueWriter, 5);
            try(DeadLetterQueueInputPlugin secondPlugin = new DeadLetterQueueInputPlugin(dir, false, null, cutoffTimestamp)) {
                assertMessagesReceived(secondPlugin, 205);
            }
        } finally {
            queueWriter.close();
        }
    }

    @Test
    public void testConsumeTwiceOffsetsNoDate() throws Exception {
        DeadLetterQueueWriter queueWriter = null;
        try {
            queueWriter = new DeadLetterQueueWriter(dir, 100000000, 10000000);
            Path since = getSinceDbPathName();
            writeEventsWithCutoff(queueWriter, 1000, 800);

            try(DeadLetterQueueInputPlugin plugin = new DeadLetterQueueInputPlugin(dir, true, since, null)) {
                assertMessagesReceived(plugin, 1000);
            }
            writeEvents(queueWriter, 5);
            try(DeadLetterQueueInputPlugin secondPlugin = new DeadLetterQueueInputPlugin(dir, true, since, null)) {
                assertMessagesReceived(secondPlugin, 5);
            }
        }finally{
            queueWriter.close();
        }
    }

    @Test
    public void testConsumeTwiceOffsetsWithDate() throws Exception {
        DeadLetterQueueWriter queueWriter = null;
        try {
            queueWriter = new DeadLetterQueueWriter(dir, 100000000, 10000000);
            Path since = getSinceDbPathName();
            Timestamp cutoffTimestamp = writeEventsWithCutoff(queueWriter, 1000, 800);

            try(DeadLetterQueueInputPlugin plugin = new DeadLetterQueueInputPlugin(dir, true, since, cutoffTimestamp)){
                assertMessagesReceived(plugin, 200);
            }

            writeEvents(queueWriter, 5);
            try(DeadLetterQueueInputPlugin secondPlugin = new DeadLetterQueueInputPlugin(dir, true, since, cutoffTimestamp)){
                assertMessagesReceived(secondPlugin, 5);
            }
        }finally{
            queueWriter.close();
        }
    }

    @Test
    public void testConsumeTwiceNoOffsetsNoDate() throws Exception {
        DeadLetterQueueWriter queueWriter = null;
        try {
            queueWriter = new DeadLetterQueueWriter(dir, 100000000, 10000000);
            writeEventsWithCutoff(queueWriter, 1000, 800);

            try(DeadLetterQueueInputPlugin plugin = new DeadLetterQueueInputPlugin(dir, false, null, null)){
                assertMessagesReceived(plugin, 1000);
            }

            writeEvents(queueWriter, 5);

            try(DeadLetterQueueInputPlugin secondPlugin = new DeadLetterQueueInputPlugin(dir, false, null, null)) {
                assertMessagesReceived(secondPlugin, 1005);
            }
        }finally{
            queueWriter.close();
        }
    }

    /**
     * Assert that the number of messages received by the plugin matches the expected count.
     * @param plugin
     * @param expectedCount
     * @throws InterruptedException
     * @throws IOException
     */
    private static void assertMessagesReceived(DeadLetterQueueInputPlugin plugin, int expectedCount) throws InterruptedException, IOException {
        LongAdder count = new LongAdder();
        Thread pluginThread = new Thread(() -> {
            try {
                plugin.register();
                plugin.run((e) -> {count.increment();});
            } catch (Exception e) {
                // do nothing
            }
        });
        pluginThread.start();
        Thread.sleep(2000);
        pluginThread.interrupt();
        pluginThread.join(1000);
        assertEquals(expectedCount, count.intValue());
    }

    /**
     * Write events to the queue, adding a boundary
     * @param queueWriter instance of {@link DeadLetterQueueWriter} to write entry to queue
     * @param eventsToWrite How many events to write in total
     * @param cutOffPoint After how many events should the 'cutoff' timestamp be written
     * @return CutOff {@link Timestamp}
     * @throws IOException
     */
    private static Timestamp writeEventsWithCutoff(DeadLetterQueueWriter queueWriter, int eventsToWrite, int cutOffPoint) throws IOException {
        long epoch = 1490659200000L;
        Timestamp cutoffTimestamp = null;
        for (int i = 0; i < eventsToWrite; i++) {
            DLQEntry entry = new DLQEntry(new Event(), "test", "test", "test", new Timestamp(epoch));
            queueWriter.writeEntry(entry);
            epoch += 1000;
            if (i == cutOffPoint) {
                cutoffTimestamp = entry.getEntryTime();
            }
        }
        return cutoffTimestamp;
    }

    /**
     * Write events to the queue
     * @param queueWriter instance of {@link DeadLetterQueueWriter} to write entry to queue
     * @param eventsToWrite How many events to write in total
     * @throws IOException
     */
    private static void writeEvents(DeadLetterQueueWriter queueWriter, int eventsToWrite) throws IOException {
        for (int i = 0; i < eventsToWrite; i++) {
            DLQEntry entry = new DLQEntry(new Event(), "test", "test", "test");
            queueWriter.writeEntry(entry);
        }
    }

    /**
     * Return the path of the since db, but do not create
     * @return {@link Path} Location of the since db.
     */
    private Path getSinceDbPathName() {
        return temporaryFolder.getRoot().toPath().resolve(".sincdb");
    }

}
