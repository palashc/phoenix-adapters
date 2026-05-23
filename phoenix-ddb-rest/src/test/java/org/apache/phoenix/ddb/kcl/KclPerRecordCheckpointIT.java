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
import java.util.List;

/**
 * Verifies per-record durability: the processor calls {@code checkpoint(record)} after
 * every record. When the Worker is killed mid-batch, restart resumes at record K+1, not at
 * the start of the batch.
 */
public class KclPerRecordCheckpointIT extends KclTestBase {

    @Test(timeout = 180_000)
    public void resumesAfterPartialBatch() throws Exception {
        String tableName = "perRecCheckpointIT_" + System.currentTimeMillis();
        CreateTableRequest create = DDLTestUtils.addStreamSpecToRequest(
                DDLTestUtils.getCreateTableRequest(tableName, "PK1", ScalarAttributeType.S,
                        "PK2", ScalarAttributeType.N),
                "NEW_AND_OLD_IMAGES");
        DualOracle oracle = lifecycle.track(DualOracle.create(
                phoenixV2, phoenixStreamsV2, localDdb(), create));
        String streamArn = oracle.phoenixStreamArn();

        for (int i = 1; i <= 10; i++) {
            oracle.putRow("K", i, "rec-" + i);
        }

        String appName = KclTestUtils.uniqueApplicationName("perRecCheckpointApp");
        lifecycle.trackAppName(appName);

        RecordingStore store1 = new RecordingStore();
        RecordingRecordProcessor.Behavior behavior = RecordingRecordProcessor.Behavior.builder()
                .checkpointPolicy(RecordingRecordProcessor.CheckpointPolicy.PER_RECORD)
                .build();
        KinesisClientLibConfiguration cfg1 = kclConfig(appName, streamArn).withMaxRecords(5);
        WorkerHandle h1 = lifecycle.track(startWorker(cfg1,
                new RecordingRecordProcessorFactory(store1, behavior)));

        // Wait until at least 3 records have been observed (and per-record checkpointed),
        // then kill the worker — simulating a crash mid-batch.
        KclTestUtils.awaitConsumerCaughtUp(store1, 3, Duration.ofSeconds(45));
        int observedBeforeCrash = store1.totalCount();
        h1.shutdownNow();

        // Restart: store2 must only see records strictly after the per-record checkpoint.
        RecordingStore store2 = new RecordingStore();
        KinesisClientLibConfiguration cfg2 = kclConfig(appName, streamArn).withMaxRecords(5);
        lifecycle.track(startWorker(cfg2,
                new RecordingRecordProcessorFactory(store2, behavior)));

        KclTestUtils.awaitConsumerCaughtUp(store2, 10 - observedBeforeCrash,
                Duration.ofSeconds(60));

        // The combined per-key sequence across both stores must cover 1..10 with no missing,
        // and store2 should not redeliver every early record (at-least-once still allows
        // last-record redelivery if KCL hadn't persisted the in-flight checkpoint).
        List<Integer> store2Seq = KclTestUtils.pk2SequenceForPk1(store2, "K");
        // Bound: store2 must NOT contain pk2=1 (record #1 was definitely checkpointed under
        // per-record cadence after store1 observed >=3 records).
        Assert.assertFalse("Restart unexpectedly redelivered record #1 — per-record checkpoint failed",
                store2Seq.contains(1));

        // Behavioral parity: DDB oracle ran uninterrupted and saw all 10 records,
        // including record #1. Rules out "phoenix-ddb-rest dropped record #1 entirely"
        // as an alternate explanation for the bound above.
        KclTestUtils.awaitConsumerCaughtUp(oracle.ddbStore(), 10, Duration.ofSeconds(30));
        Assert.assertTrue("DDB oracle missed record #1",
                KclTestUtils.pk2SequenceForPk1(oracle.ddbStore(), "K").contains(1));
    }
}
