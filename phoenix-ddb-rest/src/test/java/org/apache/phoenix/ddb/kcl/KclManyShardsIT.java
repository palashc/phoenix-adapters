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

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import org.apache.phoenix.ddb.DDLTestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

import java.time.Duration;
import java.util.Map;
import java.util.Set;

/**
 * Verifies deep shard lineage: 4 sequential splits at narrowing key ranges produce a tree
 * of shards that KCL discovers (within the configured {@code shardSyncIntervalMillis}),
 * leases one lease per leaf, and through which every write is delivered exactly once.
 *
 * <p>Splits chosen so each split is on a distinct lineage rather than cascading nested:
 * {@code "M"}, then {@code "F"} (left side), then {@code "S"} (right side), then {@code "C"}
 * (leftmost). Final leaf shard count: 5.
 */
public class KclManyShardsIT extends KclTestBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(KclManyShardsIT.class);

    @Test(timeout = 300_000)
    public void deepLineageOneLeasePerLeafShard() throws Exception {
        String tableName = "manyShardsIT_" + System.currentTimeMillis();
        CreateTableRequest create = DDLTestUtils.addStreamSpecToRequest(
                DDLTestUtils.getCreateTableRequest(tableName, "PK1", ScalarAttributeType.S,
                        "PK2", ScalarAttributeType.N),
                "NEW_AND_OLD_IMAGES");
        DualOracle oracle = lifecycle.track(DualOracle.create(
                phoenixV2, phoenixStreamsV2, localDdb(), create));
        String streamArn = oracle.phoenixStreamArn();

        // Seed records spanning all five planned leaf ranges:
        //   ranges (low..high)
        //   leaf 1: ..C        -> "AAA"
        //   leaf 2: C..F       -> "DDD"
        //   leaf 3: F..M       -> "HHH"
        //   leaf 4: M..S       -> "PPP"
        //   leaf 5: S..        -> "WWW"
        String[] pks = { "AAA", "DDD", "HHH", "PPP", "WWW" };
        for (int i = 0; i < pks.length; i++) {
            oracle.putRow(pks[i], 1, "seed-" + pks[i]);
        }

        String appName = KclTestUtils.uniqueApplicationName("manyShardsApp");
        lifecycle.trackAppName(appName);

        RecordingStore store = new RecordingStore();
        KinesisClientLibConfiguration config = kclConfig(appName, streamArn)
                .withShardSyncIntervalMillis(1000)
                .withMaxRecords(10);
        lifecycle.track(startWorker(config,
                new RecordingRecordProcessorFactory(store, RecordingRecordProcessor.Behavior.defaults())));

        KclTestUtils.awaitConsumerCaughtUp(store, 5, Duration.ofSeconds(45));

        // Sequential splits at narrowing keys. Drive a write between each so each generation
        // of daughter shards actually has data flowing through it.
        for (String split : new String[]{ "M", "F", "S", "C" }) {
            KclTestUtils.splitTableAt(jdbcUrl(), tableName, split);
            // Drive at least one write that lands on each side of the new split.
            for (int i = 0; i < pks.length; i++) {
                oracle.putRow(pks[i], pks[i].length() + split.length(),
                        "after-split-" + split + "-" + pks[i]);
            }
        }

        // Expect 5 seed + 4 splits * 5 writes = 25 records total. Loose timeout for split discovery.
        KclTestUtils.awaitConsumerCaughtUp(store, 25, Duration.ofSeconds(120));

        // Lease table: expect at least 5 leaf leases (closed parents stay in the table too,
        // checkpointed at SHARD_END, so total count is >= 5).
        Map<String, Map<String, AttributeValue>> leases =
                KclTestUtils.awaitLeaseCount(phoenixV1, appName, 5, Duration.ofSeconds(30));
        int leaves = 0;
        for (Map<String, AttributeValue> row : leases.values()) {
            AttributeValue ck = row.get(KclTestUtils.CHECKPOINT_COLUMN);
            if (ck == null || !KclTestUtils.SHARD_END.equals(ck.getS())) {
                leaves++;
            }
        }
        LOGGER.info("Lease tree: {} total, {} leaves (not SHARD_END)", leases.size(), leaves);
        Assert.assertTrue("Expected exactly 5 leaf leases, got " + leaves
                        + " (total " + leases.size() + ")",
                leaves == 5);

        // Sanity: at least 5 distinct shard IDs delivered records, matching the leaf count
        // (plus possibly some parents that drained while active).
        Set<String> shards = store.shardsSeen();
        Assert.assertTrue("Expected delivery across at least 5 shards, got " + shards,
                shards.size() >= 5);

        // DDB parity: PER_KEY_SEQUENCE — Phoenix's 4-split lineage is invisible to DDB but
        // per-key event order matches across both sides.
        KclTestUtils.awaitConsumerCaughtUp(oracle.ddbStore(), 25, Duration.ofSeconds(30));
        KclTestUtils.assertContentParity(store, oracle.ddbStore(),
                KclTestUtils.ParityMode.PER_KEY_SEQUENCE);
    }
}
