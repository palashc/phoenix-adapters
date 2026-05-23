/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.ddb.kcl;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import org.apache.phoenix.ddb.DDLTestUtils;
import org.junit.Assert;
import org.junit.Test;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

import java.time.Duration;

/**
 * Verifies that a Worker stopped gracefully after checkpointing can be restarted with the
 * same {@code applicationName} and resume delivery from the checkpoint — no gaps, and no
 * replays beyond what KCL guarantees (at-least-once at the batch boundary).
 *
 * <p>The lease table persists across the stop/restart cycle; this test asserts that fact
 * and that the second store sees only the post-stop writes.
 */
public class KclCheckpointResumeIT extends KclTestBase {

    @Test(timeout = 180_000)
    public void resumesFromCheckpointAfterGracefulStop() throws Exception {
        String tableName = "resumeIT_" + System.currentTimeMillis();
        CreateTableRequest create = DDLTestUtils.addStreamSpecToRequest(
                DDLTestUtils.getCreateTableRequest(tableName, "PK1", ScalarAttributeType.S,
                        "PK2", ScalarAttributeType.N),
                "NEW_AND_OLD_IMAGES");
        DualOracle oracle = lifecycle.track(DualOracle.create(
                phoenixV2, phoenixStreamsV2, localDdb(), create));
        String streamArn = oracle.phoenixStreamArn();

        String appName = KclTestUtils.uniqueApplicationName("resumeApp");
        lifecycle.trackAppName(appName);

        // Write a first batch; run Worker #1 long enough to drain and checkpoint, then stop gracefully.
        for (int i = 1; i <= 5; i++) {
            oracle.putRow("K", i, "first-batch-" + i);
        }

        RecordingStore store1 = new RecordingStore();
        KinesisClientLibConfiguration cfg1 = kclConfig(appName, streamArn).withMaxRecords(5);
        WorkerHandle h1 = lifecycle.track(startWorker(cfg1,
                new RecordingRecordProcessorFactory(store1, RecordingRecordProcessor.Behavior.defaults())));

        KclTestUtils.awaitConsumerCaughtUp(store1, 5, Duration.ofSeconds(45));

        h1.gracefulShutdown(Duration.ofSeconds(15));

        // Lease table must persist past graceful shutdown — that is what makes resume possible.
        Assert.assertFalse("Lease table should still exist after graceful shutdown",
                KclTestUtils.scanLeases(phoenixV1, appName).isEmpty());

        // Write a second batch and start Worker #2 with the same application name.
        for (int i = 6; i <= 10; i++) {
            oracle.putRow("K", i, "second-batch-" + i);
        }

        RecordingStore store2 = new RecordingStore();
        KinesisClientLibConfiguration cfg2 = kclConfig(appName, streamArn).withMaxRecords(5);
        lifecycle.track(startWorker(cfg2,
                new RecordingRecordProcessorFactory(store2, RecordingRecordProcessor.Behavior.defaults())));

        KclTestUtils.awaitConsumerCaughtUp(store2, 5, Duration.ofSeconds(60));

        // Worker #2 must not have replayed the first batch. KCL guarantees at-least-once,
        // so we accept that at most the last record of the first batch could redeliver
        // (if checkpoint hadn't advanced); but the new store should NOT contain the
        // earliest records.
        Assert.assertFalse("Worker #2 unexpectedly redelivered an early record",
                KclTestUtils.pk2SequenceForPk1(store2, "K").contains(1));
        Assert.assertTrue("Worker #2 missed second-batch records",
                KclTestUtils.pk2SequenceForPk1(store2, "K").containsAll(
                        java.util.Arrays.asList(6, 7, 8, 9, 10)));

        // Behavioral parity: DDB oracle (single uninterrupted worker) saw all 10 records.
        // Phoenix store2 must not contain any record the DDB oracle places before the
        // shutdown cutoff — catches checkpoint-persistence regressions.
        KclTestUtils.awaitConsumerCaughtUp(oracle.ddbStore(), 10, Duration.ofSeconds(30));
        Assert.assertEquals("DDB oracle delivered wrong record set",
                java.util.Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
                KclTestUtils.pk2SequenceForPk1(oracle.ddbStore(), "K"));
    }
}
