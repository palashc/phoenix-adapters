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
 * Verifies the graceful-shutdown checkpoint pattern. In KCL v1 the plain
 * {@code shutdown(ShutdownInput)} callback is only invoked with {@code TERMINATE} or
 * {@code ZOMBIE} — never {@code REQUESTED} — so a processor that wants a final flush on
 * cooperative stop must implement {@code IShutdownNotificationAware} (as
 * {@link RecordingRecordProcessor} does). This test exercises the {@code shutdownRequested()}
 * path and asserts that restart with the same {@code applicationName} resumes from exactly
 * that position with no redelivery.
 *
 * <p>{@code TERMINATE} from a closed shard is covered by {@link KclSplitDuringConsumeIT}'s
 * assertion that the parent lease reaches {@link KclTestUtils#SHARD_END}.
 */
public class KclShutdownCheckpointIT extends KclTestBase {

    @Test
    public void shutdownHandlerCheckpointPersists() throws Exception {
        String tableName = "shutdownIT_" + System.currentTimeMillis();
        CreateTableRequest create = DDLTestUtils.addStreamSpecToRequest(
                DDLTestUtils.getCreateTableRequest(tableName, "PK1", ScalarAttributeType.S,
                        "PK2", ScalarAttributeType.N),
                "NEW_AND_OLD_IMAGES");
        DualOracle oracle = lifecycle.track(DualOracle.create(
                phoenixV2, phoenixStreamsV2, localDdb(), create));
        String streamArn = oracle.phoenixStreamArn();

        for (int i = 1; i <= 5; i++) {
            oracle.putRow("K", i, "first-" + i);
        }

        String appName = KclTestUtils.uniqueApplicationName("shutdownApp");
        lifecycle.trackAppName(appName);

        // Behavior: never checkpoint during processRecords, only on shutdown. This isolates
        // the shutdown-handler checkpoint as the only thing that could possibly persist
        // a position.
        RecordingStore store1 = new RecordingStore();
        RecordingRecordProcessor.Behavior shutdownOnly = RecordingRecordProcessor.Behavior.builder()
                .checkpointPolicy(RecordingRecordProcessor.CheckpointPolicy.NEVER)
                .checkpointOnShutdown(true)
                .build();
        KinesisClientLibConfiguration cfg1 = kclConfig(appName, streamArn).withMaxRecords(5);
        WorkerHandle h1 = lifecycle.track(startWorker(cfg1,
                new RecordingRecordProcessorFactory(store1, shutdownOnly)));

        KclTestUtils.awaitConsumerCaughtUp(store1, 5, Duration.ofSeconds(45));

        // Worker.startGracefulShutdown's Future completes when the shutdown sequence STARTS,
        // not when shutdown(REQUESTED)'s checkpoint UpdateItem has landed. Poll the lease's
        // checkpoint column past TRIM_HORIZON before starting worker #2; otherwise worker #2
        // can take over and re-anchor to TRIM_HORIZON before our late checkpoint catches up.
        h1.gracefulShutdown(Duration.ofSeconds(20));
        Assert.assertTrue("Shutdown-handler checkpoint never landed in lease table",
                KclTestUtils.awaitTrue(() -> {
                    String ck = KclTestUtils.leaseCheckpoint(phoenixV1, appName,
                            store1.shardsSeen().iterator().next());
                    return ck != null && !"TRIM_HORIZON".equals(ck);
                }, Duration.ofSeconds(15)));

        for (int i = 6; i <= 10; i++) {
            oracle.putRow("K", i, "second-" + i);
        }

        RecordingStore store2 = new RecordingStore();
        RecordingRecordProcessor.Behavior plain = RecordingRecordProcessor.Behavior.defaults();
        KinesisClientLibConfiguration cfg2 = kclConfig(appName, streamArn).withMaxRecords(5);
        lifecycle.track(startWorker(cfg2,
                new RecordingRecordProcessorFactory(store2, plain)));

        KclTestUtils.awaitConsumerCaughtUp(store2, 5, Duration.ofSeconds(60));

        // Worker #2 must NOT redeliver record #1; the shutdown-handler checkpoint advanced
        // the lease past the first batch.
        Assert.assertFalse(
                "Restart unexpectedly redelivered an early record — shutdown checkpoint failed",
                KclTestUtils.pk2SequenceForPk1(store2, "K").contains(1));
        Assert.assertTrue("second-batch records missing on restart",
                KclTestUtils.pk2SequenceForPk1(store2, "K")
                        .containsAll(java.util.Arrays.asList(6, 7, 8, 9, 10)));

        // Behavioral parity: DDB oracle's worker is single-instance, never restarts, never
        // checkpoints-only-on-shutdown — its store is the AWS-reference union of first+second
        // batches. Phoenix-side store2 must not contain records the DDB oracle places
        // strictly before the cutoff.
        KclTestUtils.awaitConsumerCaughtUp(oracle.ddbStore(), 10, Duration.ofSeconds(30));
        Assert.assertEquals("DDB oracle delivered wrong record set",
                java.util.Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
                KclTestUtils.pk2SequenceForPk1(oracle.ddbStore(), "K"));
    }
}
