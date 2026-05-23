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
 * Counterpart to {@link KclProcessFailureIT}. Verifies the swallow-and-advance pattern
 * commonly used by KCL consumers: {@code processRecords} catches {@code Throwable} and
 * still checkpoints, so KCL does not see the throw — the poison record is silently dropped
 * and the consumer advances.
 *
 * <p>After restart with the same {@code applicationName}, the poison record is not
 * redelivered (at-most-once for failures).
 */
public class KclSwallowExceptionAdvanceIT extends KclTestBase {

    @Test(timeout = 180_000)
    public void poisonRecordIsSilentlyDropped() throws Exception {
        String tableName = "swallowIT_" + System.currentTimeMillis();
        CreateTableRequest create = DDLTestUtils.addStreamSpecToRequest(
                DDLTestUtils.getCreateTableRequest(tableName, "PK1", ScalarAttributeType.S,
                        "PK2", ScalarAttributeType.N),
                "NEW_AND_OLD_IMAGES");
        DualOracle oracle = lifecycle.track(DualOracle.create(
                phoenixV2, phoenixStreamsV2, localDdb(), create));
        String streamArn = oracle.phoenixStreamArn();

        for (int i = 1; i <= 5; i++) {
            oracle.putRow("K", i, "rec-" + i);
        }

        String appName = KclTestUtils.uniqueApplicationName("swallowApp");
        lifecycle.trackAppName(appName);

        // Position 2 (i.e. the 3rd record) is the poison: the processor will throw on it,
        // catch the exception itself, and checkpoint(record) anyway.
        RecordingStore store = new RecordingStore();
        RecordingRecordProcessor.Behavior behavior = RecordingRecordProcessor.Behavior.builder()
                .failurePolicy(RecordingRecordProcessor.poisonAt(2))
                .checkpointPolicy(RecordingRecordProcessor.CheckpointPolicy.PER_RECORD)
                .build();

        KinesisClientLibConfiguration config = kclConfig(appName, streamArn).withMaxRecords(1);
        WorkerHandle h = lifecycle.track(startWorker(config,
                new RecordingRecordProcessorFactory(store, behavior)));

        KclTestUtils.awaitConsumerCaughtUp(store, 4, Duration.ofSeconds(60));

        // The poison record (3rd written, position 2) was thrown on and not appended.
        // The other 4 records were appended.
        List<Integer> delivered = KclTestUtils.pk2SequenceForPk1(store, "K");
        Assert.assertEquals("Expected 1,2,4,5 (poison record #3 silently dropped)",
                java.util.Arrays.asList(1, 2, 4, 5), delivered);

        // Restart with same app name; poison record must not redeliver — its position was
        // checkpointed past.
        h.gracefulShutdown(Duration.ofSeconds(15));

        RecordingStore store2 = new RecordingStore();
        // Reset behavior: no more failures, just record everything.
        RecordingRecordProcessor.Behavior plain = RecordingRecordProcessor.Behavior.defaults();
        KinesisClientLibConfiguration cfg2 = kclConfig(appName, streamArn).withMaxRecords(5);
        lifecycle.track(startWorker(cfg2, new RecordingRecordProcessorFactory(store2, plain)));

        // Give the second worker time to claim a lease and (potentially) replay.
        KclTestUtils.sleepQuietly(Duration.ofSeconds(8));

        Assert.assertFalse("Poison record was redelivered on restart — at-most-once violated",
                KclTestUtils.pk2SequenceForPk1(store2, "K").contains(3));

        // Behavioral parity: DDB oracle's processor uses default behavior so its store sees
        // all 5 records uninterrupted. The Phoenix-side post-restart store should not
        // redeliver any record the DDB side already advanced past — concretely, neither
        // side ends up with the poison record in the post-restart store.
        KclTestUtils.awaitConsumerCaughtUp(oracle.ddbStore(), 5, Duration.ofSeconds(30));
        Assert.assertEquals("DDB oracle delivered wrong record set",
                java.util.Arrays.asList(1, 2, 3, 4, 5),
                KclTestUtils.pk2SequenceForPk1(oracle.ddbStore(), "K"));
    }
}
