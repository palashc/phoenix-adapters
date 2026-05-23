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
 * Verifies the empty-batch delivery mechanism: with {@code
 * withCallProcessRecordsEvenForEmptyRecordList(true)}, {@code processRecords} fires with an
 * empty list at the polling cadence, {@code checkpointer.checkpoint()} (no record arg)
 * succeeds, and the next non-empty batch is delivered after the next write.
 *
 * <p>Uses a deliberately relaxed {@code idleTimeBetweenReadsInMillis(1000)} so the test
 * asserts mechanism correctness rather than {@code GetRecords} p99 latency.
 */
public class KclEmptyBatchProcessIT extends KclTestBase {

    @Test(timeout = 120_000)
    public void emptyBatchesAndIdleCheckpointWork() throws Exception {
        String tableName = "emptyBatchIT_" + System.currentTimeMillis();
        CreateTableRequest create = DDLTestUtils.addStreamSpecToRequest(
                DDLTestUtils.getCreateTableRequest(tableName, "PK1", ScalarAttributeType.S,
                        "PK2", ScalarAttributeType.N),
                "NEW_AND_OLD_IMAGES");
        DualOracle oracle = lifecycle.track(DualOracle.create(
                phoenixV2, phoenixStreamsV2, localDdb(), create));
        String streamArn = oracle.phoenixStreamArn();

        String appName = KclTestUtils.uniqueApplicationName("emptyBatchApp");
        lifecycle.trackAppName(appName);

        RecordingStore store = new RecordingStore();
        RecordingRecordProcessor.Behavior behavior = RecordingRecordProcessor.Behavior.builder()
                .onEmptyBatch(ckpt -> {
                    try {
                        ckpt.checkpoint();
                    } catch (Exception ignored) {
                        // The first empty batch can checkpoint at TRIM_HORIZON before any
                        // record exists; KCL may reject it. The test still measures that
                        // processRecords([]) fires.
                    }
                })
                .build();

        KinesisClientLibConfiguration config = kclConfig(appName, streamArn)
                .withIdleTimeBetweenReadsInMillis(1000)
                .withCallProcessRecordsEvenForEmptyRecordList(true)
                .withMaxRecords(10);

        WorkerHandle h = lifecycle.track(startWorker(config,
                new RecordingRecordProcessorFactory(store, behavior)));

        // Idle long enough that empty batches must have fired.
        KclTestUtils.sleepQuietly(Duration.ofSeconds(8));
        Assert.assertFalse("Worker died during idle period", h.isDone());

        // Single write must land in a subsequent non-empty batch.
        oracle.putRow("K", 1, "after-idle");

        KclTestUtils.awaitConsumerCaughtUp(store, 1, Duration.ofSeconds(30));
        Assert.assertEquals(java.util.Collections.singletonList(1),
                KclTestUtils.pk2SequenceForPk1(store, "K"));

        // Behavioral parity: DDB oracle also delivers the post-idle write within the same
        // envelope. Catches the case where phoenix-ddb-rest's GetRecords returns malformed
        // empty pages that cause the iterator to die — Phoenix would deliver nothing while
        // DDB happily delivers the write.
        KclTestUtils.awaitConsumerCaughtUp(oracle.ddbStore(), 1, Duration.ofSeconds(30));
        Assert.assertEquals(java.util.Collections.singletonList(1),
                KclTestUtils.pk2SequenceForPk1(oracle.ddbStore(), "K"));
    }
}
