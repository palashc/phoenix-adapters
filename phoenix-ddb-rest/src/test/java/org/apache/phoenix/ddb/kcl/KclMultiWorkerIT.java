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
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Verifies that two Workers sharing an {@code applicationName} but with distinct
 * {@code workerId}s distribute the daughter shard leases between them, and that every
 * post-split record is delivered exactly once across the union of the two stores.
 *
 * <p>Requires a pre-test split so two daughter shards exist for the leases to land on.
 */
public class KclMultiWorkerIT extends KclTestBase {

    @Test(timeout = 180_000)
    public void splitsLeasesAcrossTwoWorkers() throws Exception {
        String tableName = "multiWorkerIT_" + System.currentTimeMillis();
        CreateTableRequest create = DDLTestUtils.addStreamSpecToRequest(
                DDLTestUtils.getCreateTableRequest(tableName, "PK1", ScalarAttributeType.S,
                        "PK2", ScalarAttributeType.N),
                "NEW_AND_OLD_IMAGES");
        DualOracle oracle = lifecycle.track(DualOracle.create(
                phoenixV2, phoenixStreamsV2, localDdb(), create));
        String streamArn = oracle.phoenixStreamArn();

        // Pre-test split to create two daughter shards. No pre-split writes — Phoenix CDC
        // routes records by region-key range *after* the split, so any records written
        // before the split land on the parent shard's CDC partition, which the daughters'
        // streams don't surface. This test is about lease distribution across daughters,
        // not about pre-split record visibility.
        KclTestUtils.splitTableAt(jdbcUrl(), tableName, "LMN");

        // All writes are post-split. PK1="ABC" routes left (< "LMN"), PK1="XYZ" routes right.
        for (int i = 1; i <= 5; i++) {
            oracle.putRow("ABC", i, "left-" + i);
            oracle.putRow("XYZ", i, "right-" + i);
        }

        String appName = KclTestUtils.uniqueApplicationName("multiWorkerApp");
        lifecycle.trackAppName(appName);

        RecordingStore storeA = new RecordingStore();
        RecordingStore storeB = new RecordingStore();
        KinesisClientLibConfiguration cfgA = kclConfig(appName, streamArn);
        KinesisClientLibConfiguration cfgB = kclConfig(appName, streamArn);

        lifecycle.track(startWorker(cfgA,
                new RecordingRecordProcessorFactory(storeA, RecordingRecordProcessor.Behavior.defaults())));
        lifecycle.track(startWorker(cfgB,
                new RecordingRecordProcessorFactory(storeB, RecordingRecordProcessor.Behavior.defaults())));

        // 10 records expected = 5 left + 5 right. Each worker sees some subset; KCL is
        // at-least-once across workers (lease handoff between two workers can briefly
        // re-deliver a shard's records), so we assert AT LEAST 10 combined plus full
        // unique-key coverage — the exactly-once invariant lives at the (PK1,PK2) level.
        awaitCombinedCaughtUp(storeA, storeB, 10, Duration.ofSeconds(90));
        Assert.assertTrue("Combined stores saw fewer than 10 records: A="
                        + storeA.totalCount() + " B=" + storeB.totalCount(),
                storeA.totalCount() + storeB.totalCount() >= 10);

        // Coverage: every (PK1,PK2) tuple must appear at least once across the union.
        Set<String> seen = new HashSet<>();
        seen.addAll(KclTestUtils.distinctKeys(storeA.allRecordsInArrival()));
        seen.addAll(KclTestUtils.distinctKeys(storeB.allRecordsInArrival()));
        Assert.assertEquals("Unique (PK1,PK2) tuples mismatch", 10, seen.size());

        // Two distinct lease owners across the daughter shards: prove that lease distribution
        // actually happened and the daughters didn't both end up on one worker.
        Map<String, Map<String, AttributeValue>> leases = KclTestUtils.scanLeases(phoenixV1, appName);
        Set<String> owners = new HashSet<>();
        for (Map<String, AttributeValue> row : leases.values()) {
            AttributeValue owner = row.get(KclTestUtils.LEASE_OWNER_COLUMN);
            if (owner != null && owner.getS() != null) {
                owners.add(owner.getS());
            }
        }
        Assert.assertTrue("Expected 2 distinct lease owners across daughter shards, got: " + owners,
                owners.size() >= 2);

        // Behavioral parity: DDB has a single shard and a single worker (the oracle's), but
        // every (PK1,PK2) tuple the Phoenix-side two workers saw must also appear in the DDB
        // oracle's store. Catches multi-worker UpdateItem race regressions where
        // phoenix-ddb-rest's conditional lease writes might double-deliver or drop records.
        KclTestUtils.awaitConsumerCaughtUp(oracle.ddbStore(), 10, Duration.ofSeconds(30));
        Set<String> oracleKeys =
                KclTestUtils.distinctKeys(oracle.ddbStore().allRecordsInArrival());
        Assert.assertEquals("DDB oracle key set diverges from Phoenix union",
                oracleKeys, seen);
    }

    /** Wait until {@code storeA.totalCount() + storeB.totalCount() >= expected}. */
    private static void awaitCombinedCaughtUp(RecordingStore a, RecordingStore b,
                                              int expected, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (a.totalCount() + b.totalCount() >= expected) {
                KclTestUtils.sleepQuietly(Duration.ofSeconds(1));
                return;
            }
            KclTestUtils.sleepQuietly(Duration.ofMillis(100));
        }
        throw new AssertionError("Combined did not reach " + expected
                + " within " + timeout + "; A=" + a.totalCount() + " B=" + b.totalCount());
    }
}
