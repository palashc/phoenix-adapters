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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

import java.time.Duration;

/**
 * Verifies that a single KCL Worker started from {@code TRIM_HORIZON} delivers every record
 * written to the stream — including writes made on both sides of an HBase region split that
 * occurs mid-test — and that per-partition-key order matches write order.
 *
 * <p>This is the most fundamental correctness contract of the rig: in-order delivery across
 * a shard split. It exercises {@code DescribeStream}, {@code GetShardIterator}, {@code
 * GetRecords}, the parent-before-child handshake, and the lease table from end to end.
 */
public class KclSingleWorkerTrimHorizonIT extends KclTestBase {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(KclSingleWorkerTrimHorizonIT.class);

    @Test(timeout = 180_000)
    public void deliversAllRecordsInOrderAcrossASplit() throws Exception {
        String tableName = "trimHorizonIT_" + System.currentTimeMillis();
        CreateTableRequest create = DDLTestUtils.addStreamSpecToRequest(
                DDLTestUtils.getCreateTableRequest(tableName, "PK1", ScalarAttributeType.S,
                        "PK2", ScalarAttributeType.N),
                "NEW_AND_OLD_IMAGES");
        DualOracle oracle = lifecycle.track(DualOracle.create(
                phoenixV2, phoenixStreamsV2, localDdb(), create));
        String streamArn = oracle.phoenixStreamArn();

        // Pre-split writes — both sides of the planned split key "LMN".
        oracle.putRow("ABC", 1, "pre-split-left-1");
        oracle.putRow("ABC", 2, "pre-split-left-2");
        oracle.putRow("XYZ", 1, "pre-split-right-1");

        RecordingStore store = new RecordingStore();
        String appName = KclTestUtils.uniqueApplicationName("trimHorizonApp");
        lifecycle.trackAppName(appName);
        KinesisClientLibConfiguration config = kclConfig(appName, streamArn)
                .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON)
                .withMaxRecords(10);

        lifecycle.track(startWorker(config,
                new RecordingRecordProcessorFactory(store, RecordingRecordProcessor.Behavior.defaults())));

        KclTestUtils.awaitConsumerCaughtUp(store, 3, Duration.ofSeconds(45));

        // Split the table at "LMN" — "ABC" lands left, "XYZ" lands right.
        KclTestUtils.splitTableAt(jdbcUrl(), tableName, "LMN");

        // Post-split writes — must arrive on the two daughter shards.
        oracle.putRow("ABC", 3, "post-split-left-3");
        oracle.putRow("DEF", 1, "post-split-left-4");
        oracle.putRow("XYZ", 2, "post-split-right-5");
        oracle.putRow("XYZ", 3, "post-split-right-6");

        KclTestUtils.awaitConsumerCaughtUp(store, 7, Duration.ofSeconds(60));
        LOGGER.info("Per-shard counts: {}", store.countByShard());

        // Per-key write order: ABC -> 1,2,3 ; XYZ -> 1,2,3 ; DEF -> 1.
        Assert.assertEquals(java.util.Arrays.asList(1, 2, 3),
                KclTestUtils.pk2SequenceForPk1(store, "ABC"));
        Assert.assertEquals(java.util.Arrays.asList(1, 2, 3),
                KclTestUtils.pk2SequenceForPk1(store, "XYZ"));
        Assert.assertEquals(java.util.Collections.singletonList(1),
                KclTestUtils.pk2SequenceForPk1(store, "DEF"));

        // Two HBase regions => at least two CDC shards (parent + at least one daughter).
        Assert.assertTrue("Expected >=2 shards seen: " + store.shardsSeen(),
                store.shardsSeen().size() >= 2);

        // DDB parity: PER_KEY_SEQUENCE — Phoenix splits, DDB doesn't, but per-key order is
        // topology-invariant.
        KclTestUtils.awaitConsumerCaughtUp(oracle.ddbStore(), 7, Duration.ofSeconds(30));
        KclTestUtils.assertContentParity(store, oracle.ddbStore(),
                KclTestUtils.ParityMode.PER_KEY_SEQUENCE);
    }
}
