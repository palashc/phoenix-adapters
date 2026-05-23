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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Higher-volume soak that mixes all three CDC event types ({@code INSERT}, {@code MODIFY},
 * {@code REMOVE}) and five sequential HBase splits, then asserts a KCL consumer with
 * {@code NEW_AND_OLD_IMAGES} sees every mutation with the correct event name and image
 * shape. Total events: 3,200 across six leaf shards; ~6,400 dual-write RPCs counting the
 * DDB-side mirror.
 *
 * <ul>
 *   <li>{@code GetRecords} pages many times under load and the processor batches +
 *       checkpoints repeatedly — exercising the batch-boundary checkpoint cadence.</li>
 *   <li>Five splits at varying key-space depth catch any regression where parent shard
 *       drain ordering or daughter discovery cadence depends on the absolute number of
 *       in-flight records.</li>
 *   <li>DDB parity (single-shard oracle) cross-checks the entire mutation stream against
 *       the AWS-reference behavior under high load.</li>
 * </ul>
 *
 * <p>Per-event assertions:
 * <ul>
 *   <li>{@code INSERT} — has {@code newImage}, no {@code oldImage}.</li>
 *   <li>{@code MODIFY} — has both {@code newImage} and {@code oldImage}, and the
 *       {@code newImage.payload} reflects the latest update for that key.</li>
 *   <li>{@code REMOVE} — has {@code oldImage}, no {@code newImage}.</li>
 * </ul>
 */
public class KclMixedMutationSoakIT extends KclTestBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(KclMixedMutationSoakIT.class);

    private static final int KEY_SPACE_SIZE = 500;      // 500 distinct (PK1,PK2) tuples
    private static final int UPDATES_PER_KEY = 5;       // 2500 MODIFYs
    private static final int DELETES_TO_PERFORM = 200;  // 200 REMOVEs at the end
    // Total events = 500 INSERTs + 2500 MODIFYs + 200 REMOVEs = 3200

    @Test(timeout = 600_000)
    public void mixedInsertModifyRemoveAcrossSplits() throws Exception {
        String tableName = "mixedMutationIT_" + System.currentTimeMillis();
        CreateTableRequest create = DDLTestUtils.addStreamSpecToRequest(
                DDLTestUtils.getCreateTableRequest(tableName, "PK1", ScalarAttributeType.S,
                        "PK2", ScalarAttributeType.N),
                "NEW_AND_OLD_IMAGES");
        DualOracle oracle = lifecycle.track(DualOracle.create(
                phoenixV2, phoenixStreamsV2, localDdb(), create));
        String streamArn = oracle.phoenixStreamArn();

        String appName = KclTestUtils.uniqueApplicationName("mixedMutationApp");
        lifecycle.trackAppName(appName);

        RecordingStore store = new RecordingStore();
        KinesisClientLibConfiguration config = kclConfig(appName, streamArn)
                .withMaxRecords(100);

        lifecycle.track(startWorker(config,
                new RecordingRecordProcessorFactory(store, RecordingRecordProcessor.Behavior.defaults())));

        // Wait for the Worker to claim a lease so its TRIM_HORIZON anchor catches the
        // upcoming writes.
        Assert.assertTrue("Worker never claimed a lease",
                KclTestUtils.awaitWorkerReady(phoenixV1, appName, Duration.ofSeconds(60)));

        // Phase 1: INSERT every key. PK1 prefixes straddle every planned split point so
        // every leaf shard receives writes; PK2 is just an integer suffix.
        String[] pk1Pool = generatePk1Pool();
        Map<String, Integer> latestVersionPerKey = new HashMap<>();
        for (int i = 0; i < KEY_SPACE_SIZE; i++) {
            String pk1 = pk1Pool[i % pk1Pool.length];
            int pk2 = i;
            putRow(oracle, tableName, pk1, pk2, 1, "insert-v1");
            latestVersionPerKey.put(keyOf(pk1, pk2), 1);
        }

        // Split #1: roughly halve the key space.
        KclTestUtils.splitTableAt(jdbcUrl(), tableName, "m");

        // Phase 2: MODIFY every key UPDATES_PER_KEY times. Each update bumps a `version`
        // attribute and changes `payload` so we can verify the latest MODIFY's newImage
        // payload reflects the highest version. Four more splits are interleaved between
        // modify rounds so daughters keep emerging while there are in-flight records.
        for (int round = 2; round <= 1 + UPDATES_PER_KEY; round++) {
            for (int i = 0; i < KEY_SPACE_SIZE; i++) {
                String pk1 = pk1Pool[i % pk1Pool.length];
                int pk2 = i;
                updateRow(oracle, tableName, pk1, pk2, round, "modify-v" + round);
                latestVersionPerKey.put(keyOf(pk1, pk2), round);
            }
            if (round == 2) {
                KclTestUtils.splitTableAt(jdbcUrl(), tableName, "f");  // narrow leftmost
            }
            if (round == 3) {
                KclTestUtils.splitTableAt(jdbcUrl(), tableName, "s");  // narrow rightmost
            }
            if (round == 4) {
                KclTestUtils.splitTableAt(jdbcUrl(), tableName, "c");  // further left
            }
            if (round == 5) {
                KclTestUtils.splitTableAt(jdbcUrl(), tableName, "p");  // further right
            }
        }

        // Phase 3: REMOVE the first DELETES_TO_PERFORM keys.
        for (int i = 0; i < DELETES_TO_PERFORM; i++) {
            String pk1 = pk1Pool[i % pk1Pool.length];
            int pk2 = i;
            deleteRow(oracle, tableName, pk1, pk2);
        }

        int expectedInserts = KEY_SPACE_SIZE;
        int expectedModifies = KEY_SPACE_SIZE * UPDATES_PER_KEY;
        int expectedRemoves = DELETES_TO_PERFORM;
        int expectedTotal = expectedInserts + expectedModifies + expectedRemoves;

        KclTestUtils.awaitConsumerCaughtUp(store, expectedTotal, Duration.ofSeconds(300));

        // Bucket events by type and assert per-bucket invariants.
        List<com.amazonaws.services.dynamodbv2.model.Record> all = store.allRecordsInArrival();
        Map<String, Long> byEventName = all.stream().collect(
                Collectors.groupingBy(r -> r.getEventName(), Collectors.counting()));
        LOGGER.info("Event counts: {}", byEventName);
        Assert.assertEquals("INSERT count mismatch",
                Long.valueOf(expectedInserts), byEventName.getOrDefault("INSERT", 0L));
        Assert.assertEquals("MODIFY count mismatch",
                Long.valueOf(expectedModifies), byEventName.getOrDefault("MODIFY", 0L));
        Assert.assertEquals("REMOVE count mismatch",
                Long.valueOf(expectedRemoves), byEventName.getOrDefault("REMOVE", 0L));

        // Per-event image-shape invariants.
        for (com.amazonaws.services.dynamodbv2.model.Record r : all) {
            String eventName = r.getEventName();
            Map<String, com.amazonaws.services.dynamodbv2.model.AttributeValue> newImg =
                    r.getDynamodb().getNewImage();
            Map<String, com.amazonaws.services.dynamodbv2.model.AttributeValue> oldImg =
                    r.getDynamodb().getOldImage();
            switch (eventName) {
                case "INSERT":
                    Assert.assertNotNull("INSERT must have newImage: " + r, newImg);
                    Assert.assertNull("INSERT must NOT have oldImage: " + r, oldImg);
                    break;
                case "MODIFY":
                    Assert.assertNotNull("MODIFY must have newImage: " + r, newImg);
                    Assert.assertNotNull("MODIFY must have oldImage: " + r, oldImg);
                    break;
                case "REMOVE":
                    Assert.assertNull("REMOVE must NOT have newImage: " + r, newImg);
                    Assert.assertNotNull("REMOVE must have oldImage: " + r, oldImg);
                    break;
                default:
                    Assert.fail("Unexpected event name: " + eventName + " on record " + r);
            }
        }

        // Per-key MODIFY chain: KCL per-shard ordering + single partition key → single shard
        // means arrival order == write order per key. Deleted keys are equivalent here because
        // {@link #versionsForKey} skips REMOVE events (no observable version on REMOVE).
        List<Integer> expected = java.util.stream.IntStream.rangeClosed(1, 1 + UPDATES_PER_KEY)
                .boxed().collect(Collectors.toList());
        for (Map.Entry<String, Integer> e : latestVersionPerKey.entrySet()) {
            String[] parts = e.getKey().split(":");
            String pk1 = parts[0];
            int pk2 = Integer.parseInt(parts[1]);
            Assert.assertEquals("Version chain mismatch for " + e.getKey(),
                    expected, versionsForKey(all, pk1, pk2));
        }

        // Multi-shard delivery: five splits on a single seed region => 6 leaf shards
        // (+ 5 closed parents). The consumer should see records across all leaf shards.
        LOGGER.info("Per-shard counts: {}", store.countByShard());
        Assert.assertTrue("Expected delivery across >=6 shards, saw " + store.shardsSeen(),
                store.shardsSeen().size() >= 6);

        // DDB parity: PER_KEY_SEQUENCE — Phoenix splits 5x, DDB stays single-shard, but
        // per-key INSERT → MODIFY × N → REMOVE chain must match across both sides.
        KclTestUtils.awaitConsumerCaughtUp(oracle.ddbStore(), expectedTotal,
                Duration.ofSeconds(120));
        KclTestUtils.assertContentParity(store, oracle.ddbStore(),
                KclTestUtils.ParityMode.PER_KEY_SEQUENCE);
    }

    private static void putRow(DualOracle oracle, String tableName, String pk1, int pk2,
                               int version, String payload) {
        Map<String, AttributeValue> item = baseItem(pk1, pk2);
        item.put("version", AttributeValue.builder().n(Integer.toString(version)).build());
        item.put("payload", AttributeValue.builder().s(payload).build());
        oracle.putItem(PutItemRequest.builder().tableName(tableName).item(item).build());
    }

    private static void updateRow(DualOracle oracle, String tableName, String pk1, int pk2,
                                  int version, String payload) {
        Map<String, AttributeValue> key = baseItem(pk1, pk2);
        Map<String, String> attrNames = new HashMap<>();
        attrNames.put("#v", "version");
        attrNames.put("#p", "payload");
        Map<String, AttributeValue> attrVals = new HashMap<>();
        attrVals.put(":v", AttributeValue.builder().n(Integer.toString(version)).build());
        attrVals.put(":p", AttributeValue.builder().s(payload).build());
        oracle.updateItem(UpdateItemRequest.builder()
                .tableName(tableName)
                .key(key)
                .updateExpression("SET #v = :v, #p = :p")
                .expressionAttributeNames(attrNames)
                .expressionAttributeValues(attrVals)
                .build());
    }

    private static void deleteRow(DualOracle oracle, String tableName, String pk1, int pk2) {
        oracle.deleteItem(DeleteItemRequest.builder()
                .tableName(tableName)
                .key(baseItem(pk1, pk2))
                .build());
    }

    private static Map<String, AttributeValue> baseItem(String pk1, int pk2) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("PK1", AttributeValue.builder().s(pk1).build());
        item.put("PK2", AttributeValue.builder().n(Integer.toString(pk2)).build());
        return item;
    }

    /**
     * Eight letter-prefix buckets, at least one per post-split range so every leaf shard
     * sees writes. Splits land at c, f, m, p, s.
     */
    private static String[] generatePk1Pool() {
        return new String[]{ "a", "d", "e", "h", "n", "o", "r", "u" };
    }

    private static String keyOf(String pk1, int pk2) {
        return pk1 + ":" + pk2;
    }

    private static List<Integer> versionsForKey(
            List<com.amazonaws.services.dynamodbv2.model.Record> all, String pk1, int pk2) {
        return all.stream()
                .filter(r -> pk1.equals(KclTestUtils.pk1Of(r)) && pk2 == KclTestUtils.pk2Of(r))
                .filter(r -> r.getDynamodb().getNewImage() != null) // skip REMOVE events
                .map(r -> Integer.parseInt(
                        r.getDynamodb().getNewImage().get("version").getN()))
                .collect(Collectors.toList());
    }
}
