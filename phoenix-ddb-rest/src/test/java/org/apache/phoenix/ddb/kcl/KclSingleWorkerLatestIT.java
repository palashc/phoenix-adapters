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

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import org.apache.phoenix.ddb.DDLTestUtils;
import org.junit.Assert;
import org.junit.Test;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

import java.time.Duration;

/**
 * Verifies that a Worker started with {@link InitialPositionInStream#LATEST} skips records
 * written before it came online and only delivers records written afterward.
 */
public class KclSingleWorkerLatestIT extends KclTestBase {

    @Test(timeout = 240_000)
    public void skipsHistoricalRecords() throws Exception {
        String tableName = "latestIT_" + System.currentTimeMillis();
        CreateTableRequest create = DDLTestUtils.addStreamSpecToRequest(
                DDLTestUtils.getCreateTableRequest(tableName, "PK1", ScalarAttributeType.S,
                        "PK2", ScalarAttributeType.N),
                "NEW_AND_OLD_IMAGES");
        DualOracle oracle = lifecycle.track(DualOracle.create(
                phoenixV2, phoenixStreamsV2, localDdb(), create));
        String streamArn = oracle.phoenixStreamArn();

        // Historical writes — must NOT be seen by a LATEST consumer. (The oracle's worker
        // is TRIM_HORIZON, so it will see these on the DDB side — they'll be excluded from
        // parity comparison via the per-key sentinel below.)
        oracle.putRow("HIST", 1, "before-worker-1");
        oracle.putRow("HIST", 2, "before-worker-2");

        RecordingStore store = new RecordingStore();
        String appName = KclTestUtils.uniqueApplicationName("latestApp");
        lifecycle.trackAppName(appName);

        KinesisClientLibConfiguration config = kclConfig(appName, streamArn)
                .withInitialPositionInStream(InitialPositionInStream.LATEST);

        lifecycle.track(startWorker(config,
                new RecordingRecordProcessorFactory(store, RecordingRecordProcessor.Behavior.defaults())));

        // Wait until KCL has actually claimed the lease.
        Assert.assertTrue("KCL Worker never claimed a lease",
                KclTestUtils.awaitWorkerReady(phoenixV1, appName, Duration.ofSeconds(120)));

        // Anchor probe: lease ownership only means KCL claimed the lease, NOT that
        // GetShardIterator(LATEST) has been issued (those happen in different KCL phases).
        // On a slow CI the anchor can be placed after a single warmup write, silently
        // losing it. Burst-write WARMUP records on a heartbeat until one is observed —
        // since LATEST captures "everything from now on", any warmup written after the
        // anchor is guaranteed to land. WARMUP is excluded from LIVE assertions.
        boolean anchored = false;
        for (int probeIdx = 0; probeIdx < 30; probeIdx++) {
            oracle.putRow("WARMUP", probeIdx, "anchor-probe-" + probeIdx);
            if (KclTestUtils.awaitTrue(
                    () -> !KclTestUtils.pk2SequenceForPk1(store, "WARMUP").isEmpty(),
                    Duration.ofSeconds(2))) {
                anchored = true;
                break;
            }
        }
        Assert.assertTrue("LATEST anchor never advanced past any warmup probe", anchored);

        // Live writes — must be seen.
        oracle.putRow("LIVE", 1, "after-worker-1");
        oracle.putRow("LIVE", 2, "after-worker-2");
        oracle.putRow("LIVE", 3, "after-worker-3");

        // The total record count includes a variable number of WARMUP probes; assert
        // on the test's actual invariants: LIVE arrives in order and HIST is excluded.
        Assert.assertTrue("Phoenix never saw all 3 LIVE records",
                KclTestUtils.awaitTrue(
                        () -> KclTestUtils.pk2SequenceForPk1(store, "LIVE").size() >= 3,
                        Duration.ofSeconds(60)));
        Assert.assertEquals(java.util.Arrays.asList(1, 2, 3),
                KclTestUtils.pk2SequenceForPk1(store, "LIVE"));
        Assert.assertTrue("Historical records leaked through LATEST anchor",
                KclTestUtils.pk2SequenceForPk1(store, "HIST").isEmpty());

        // DDB parity: oracle's worker is TRIM_HORIZON so it sees HIST + WARMUPs + LIVE.
        // Compare only the LIVE sequence — that's invariant to warmup-probe count.
        Assert.assertTrue("DDB never saw all 3 LIVE records",
                KclTestUtils.awaitTrue(
                        () -> KclTestUtils.pk2SequenceForPk1(oracle.ddbStore(), "LIVE").size() >= 3,
                        Duration.ofSeconds(30)));
        Assert.assertEquals("DDB LIVE sequence diverges",
                KclTestUtils.pk2SequenceForPk1(oracle.ddbStore(), "LIVE"),
                KclTestUtils.pk2SequenceForPk1(store, "LIVE"));
    }
}
