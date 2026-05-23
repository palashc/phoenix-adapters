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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.ddb.TestUtils;
import org.apache.phoenix.ddb.utils.PhoenixUtils;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ListStreamsRequest;
import software.amazon.awssdk.services.dynamodb.model.ListStreamsResponse;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;

import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

/**
 * Test-only helpers shared by all KCL integration tests: awaits with deadlines, KCL lease
 * table introspection (checkpoints, owners, SHARD_END), and Phoenix-side orchestration
 * (split, ARN resolution, write helpers).
 */
public final class KclTestUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(KclTestUtils.class);

    /** KCL sentinel written to {@code checkpoint} when a shard is fully drained. */
    public static final String SHARD_END = "SHARD_END";

    public static final String LEASE_KEY_COLUMN = "leaseKey";
    public static final String CHECKPOINT_COLUMN = "checkpoint";
    public static final String LEASE_OWNER_COLUMN = "leaseOwner";

    private KclTestUtils() {
    }

    // ---------------------------------------------------------------- awaits

    /** Poll {@code condition} every 100ms until true or {@code timeout} elapses. */
    public static boolean awaitTrue(BooleanSupplier condition, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            try {
                if (condition.getAsBoolean()) {
                    return true;
                }
                Thread.sleep(100);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return condition.getAsBoolean();
    }

    // -------------------------------------------------- lease-table helpers

    /** {@code checkpoint} for {@code shardId}'s lease row, or null if missing. */
    public static String leaseCheckpoint(AmazonDynamoDB ddb, String applicationName, String shardId) {
        Map<String, AttributeValue> key = Collections.singletonMap(
                LEASE_KEY_COLUMN, new AttributeValue().withS(shardId));
        try {
            GetItemResult res = ddb.getItem(new GetItemRequest()
                    .withTableName(applicationName)
                    .withKey(key)
                    .withConsistentRead(true));
            if (res.getItem() == null || !res.getItem().containsKey(CHECKPOINT_COLUMN)) {
                return null;
            }
            return res.getItem().get(CHECKPOINT_COLUMN).getS();
        } catch (ResourceNotFoundException notFound) {
            return null;
        }
    }

    /** Snapshot of every lease row as {@code shardId -> (column -> value)}. */
    public static Map<String, Map<String, AttributeValue>> scanLeases(
            AmazonDynamoDB ddb, String applicationName) {
        Map<String, Map<String, AttributeValue>> out = new LinkedHashMap<>();
        try {
            Map<String, AttributeValue> exclusiveStart = null;
            do {
                ScanRequest req = new ScanRequest()
                        .withTableName(applicationName)
                        .withConsistentRead(true)
                        .withExclusiveStartKey(exclusiveStart);
                ScanResult res = ddb.scan(req);
                for (Map<String, AttributeValue> row : res.getItems()) {
                    AttributeValue id = row.get(LEASE_KEY_COLUMN);
                    if (id != null && id.getS() != null) {
                        out.put(id.getS(), row);
                    }
                }
                exclusiveStart = res.getLastEvaluatedKey();
                if (exclusiveStart != null && exclusiveStart.isEmpty()) {
                    exclusiveStart = null;
                }
            } while (exclusiveStart != null);
        } catch (ResourceNotFoundException notFound) {
            return Collections.emptyMap();
        }
        return out;
    }

    /** Best-effort lease table delete for test teardown isolation. */
    public static void deleteLeaseTableQuietly(AmazonDynamoDB ddb, String applicationName) {
        try {
            ddb.deleteTable(new DeleteTableRequest(applicationName));
        } catch (ResourceNotFoundException ignored) {
            /* never created — likely a test failure before Worker startup */
        } catch (RuntimeException e) {
            LOGGER.warn("Failed to delete lease table {}: {}", applicationName, e.toString());
        }
    }

    /** Block until KCL has at least {@code expectedShardCount} leases; returns last snapshot. */
    public static Map<String, Map<String, AttributeValue>> awaitLeaseCount(
            AmazonDynamoDB ddb, String applicationName, int expectedShardCount, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        Map<String, Map<String, AttributeValue>> last = Collections.emptyMap();
        while (System.nanoTime() < deadline) {
            last = scanLeases(ddb, applicationName);
            if (last.size() >= expectedShardCount) {
                return last;
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return last;
            }
        }
        return last;
    }

    /**
     * Block until at least one lease has a {@code leaseOwner} set. Indicates KCL has
     * claimed the shard's lease — but NOT that {@code GetShardIterator(LATEST)} has been
     * issued; LATEST-position tests must also burst-write a sentinel anchor probe and
     * wait for it to surface before assuming "live" writes will be delivered.
     */
    public static boolean awaitWorkerReady(
            AmazonDynamoDB ddb, String applicationName, Duration timeout) {
        return awaitTrue(() -> {
            Map<String, Map<String, AttributeValue>> leases = scanLeases(ddb, applicationName);
            if (leases.isEmpty()) {
                return false;
            }
            for (Map<String, AttributeValue> row : leases.values()) {
                AttributeValue owner = row.get(LEASE_OWNER_COLUMN);
                if (owner != null && owner.getS() != null && !owner.getS().isEmpty()) {
                    return true;
                }
            }
            return false;
        }, timeout);
    }

    // -------------------------------------------------- phoenix orchestration

    /** Resolve {@code tableName}'s stream ARN via {@code ListStreams}. */
    public static String streamArnFor(DynamoDbStreamsClient streams, String tableName) {
        ListStreamsResponse resp = streams.listStreams(
                ListStreamsRequest.builder().tableName(tableName).build());
        if (resp.streams().isEmpty()) {
            throw new IllegalStateException("No stream found for table " + tableName);
        }
        return resp.streams().get(0).streamArn();
    }

    /** Split {@code tableName}'s HBase region at {@code splitRowKey}, blocking on completion. */
    public static void splitTableAt(String jdbcUrl, String tableName, String splitRowKey)
            throws Exception {
        try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
            TestUtils.splitTable(conn,
                    PhoenixUtils.getFullTableName(tableName, false),
                    Bytes.toBytes(splitRowKey));
        }
    }

    /** PUT one item with String {@code PK1} + numeric {@code PK2} and an opaque payload. */
    public static void putRow(DynamoDbClient client, String tableName,
                              String pk1, int pk2, String payload) {
        Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> item =
                new HashMap<>();
        item.put("PK1", software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder()
                .s(pk1).build());
        item.put("PK2", software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder()
                .n(Integer.toString(pk2)).build());
        item.put("payload", software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder()
                .s(payload).build());
        client.putItem(PutItemRequest.builder().tableName(tableName).item(item).build());
    }

    /** Unique app/lease-table name to avoid cross-run collisions. */
    public static String uniqueApplicationName(String prefix) {
        return prefix + "_" + System.currentTimeMillis() + "_"
                + Integer.toHexString(System.identityHashCode(new Object()));
    }

    /** PK1 (string) from a captured record's Keys map. */
    public static String pk1Of(com.amazonaws.services.dynamodbv2.model.Record record) {
        AttributeValue v = record.getDynamodb().getKeys().get("PK1");
        return v == null ? null : v.getS();
    }

    /** PK2 (int) from a captured record's Keys map. */
    public static int pk2Of(com.amazonaws.services.dynamodbv2.model.Record record) {
        AttributeValue v = record.getDynamodb().getKeys().get("PK2");
        return v == null ? -1 : Integer.parseInt(v.getN());
    }

    /** Distinct {@code (PK1, PK2)} tuples — for order-independent "did we see every write" checks. */
    public static Set<String> distinctKeys(List<com.amazonaws.services.dynamodbv2.model.Record> records) {
        Set<String> out = new HashSet<>();
        for (com.amazonaws.services.dynamodbv2.model.Record r : records) {
            out.add(pk1Of(r) + ":" + pk2Of(r));
        }
        return out;
    }

    /**
     * Ordered PK2 values observed for {@code pk1} across all shards. KCL guarantees per-shard
     * order, and a partition key hashes to a single shard, so this list must match write order.
     */
    public static List<Integer> pk2SequenceForPk1(RecordingStore store, String pk1) {
        List<Integer> out = new java.util.ArrayList<>();
        for (com.amazonaws.services.dynamodbv2.model.Record r : store.allRecordsInArrival()) {
            if (pk1.equals(pk1Of(r))) {
                out.add(pk2Of(r));
            }
        }
        return out;
    }

    /** Sleep, restoring interrupt status on interrupt. */
    public static void sleepQuietly(Duration d) {
        try {
            Thread.sleep(d.toMillis());
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    /** After {@code count >= expected}, wait this long for spurious extras to surface. */
    private static final Duration CAUGHT_UP_STABILITY_WINDOW = Duration.ofSeconds(1);
    /** For open-ended quiescence: count must be unchanged for this long. */
    private static final Duration QUIESCENCE_IDLE_WINDOW = Duration.ofSeconds(5);

    /**
     * Block until {@code store.totalCount() >= expected} (then a short stability window to
     * catch spurious extras), or fail with a diagnostic if the timeout elapses without
     * reaching that count. Returns the final count.
     *
     * <p>Use this in any test where the producer wrote a known number of records. The test
     * driver knows what was written; the consumer's loop doesn't — this helper just makes
     * the test's "wait then assert" pattern explicit in one call.
     */
    public static int awaitConsumerCaughtUp(RecordingStore store, int expected,
                                            Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (store.totalCount() >= expected) {
                sleepQuietly(CAUGHT_UP_STABILITY_WINDOW);
                return store.totalCount();
            }
            sleepQuietly(Duration.ofMillis(100));
        }
        throw new AssertionError(String.format(
                "Consumer did not see expected %d records within %s; saw %d across shards %s",
                expected, timeout, store.totalCount(), store.shardsSeen()));
    }

    /**
     * Block until {@code store.totalCount()} stops changing for {@link #QUIESCENCE_IDLE_WINDOW},
     * or until {@code overallTimeout} elapses. Returns the count observed at that point.
     *
     * <p>Time-based heuristic — only use when the producer's write count is genuinely unknown
     * (e.g. an open-ended background producer thread). For deterministic-count tests, prefer
     * {@link #awaitConsumerCaughtUp} which fails fast on shortfall.
     */
    public static int awaitQuiescence(RecordingStore store, Duration overallTimeout) {
        long deadline = System.nanoTime() + overallTimeout.toNanos();
        long idleNs = QUIESCENCE_IDLE_WINDOW.toNanos();
        int lastCount = store.totalCount();
        // Do not start the idle countdown until at least one record has arrived.
        long lastChange = lastCount > 0 ? System.nanoTime() : -1L;
        while (System.nanoTime() < deadline) {
            sleepQuietly(Duration.ofMillis(100));
            int now = store.totalCount();
            if (now != lastCount) {
                lastCount = now;
                lastChange = System.nanoTime();
            } else if (lastChange > 0 && System.nanoTime() - lastChange >= idleNs) {
                return now;
            }
        }
        return store.totalCount();
    }

    // -------------------------------------------------- content parity vs DDB

    /**
     * Granularity at which {@link #assertContentParity} compares the Phoenix and DDB stores.
     * Picked based on the topology the test produces.
     */
    public enum ParityMode {
        /**
         * Both sides have a single shard; the full record list must match position-for-position.
         * Safe only when no Phoenix-side split happens (DDB never splits).
         */
        STRICT_TOTAL_ORDER,
        /**
         * For every {@code PK1}, the ordered list of records must match. Survives Phoenix-side
         * splits because per-key ordering is topology-invariant — a partition key hashes to one
         * shard at a time and KCL drains parent before any daughter.
         */
        PER_KEY_SEQUENCE
    }

    /**
     * Assert that the Phoenix and DDB KCL Workers agree on the comparable record fields
     * (see {@link #assertRecordSequenceEquals}). Mirrors the direct-equality pattern of
     * {@link TestUtils#validateRecords} on V2 records that the existing GetRecords parity
     * ITs already rely on.
     */
    public static void assertContentParity(RecordingStore phoenixStore,
                                           RecordingStore ddbStore, ParityMode mode) {
        Assert.assertEquals("Parity total record count mismatch",
                ddbStore.totalCount(), phoenixStore.totalCount());

        List<Record> phoenix = phoenixStore.allRecordsInArrival();
        List<Record> ddb = ddbStore.allRecordsInArrival();

        switch (mode) {
            case STRICT_TOTAL_ORDER:
                assertRecordSequenceEquals("strict", phoenix, ddb);
                break;
            case PER_KEY_SEQUENCE:
                Map<String, List<Record>> phoenixByKey = groupByPk1(phoenix);
                Map<String, List<Record>> ddbByKey = groupByPk1(ddb);
                Assert.assertEquals("Per-key parity: distinct PK1 set differs",
                        ddbByKey.keySet(), phoenixByKey.keySet());
                for (String pk1 : ddbByKey.keySet()) {
                    assertRecordSequenceEquals("PK1=" + pk1,
                            phoenixByKey.get(pk1), ddbByKey.get(pk1));
                }
                break;
            default:
                throw new IllegalArgumentException("Unknown ParityMode: " + mode);
        }
    }

    /**
     * Compare two ordered record lists position-by-position, asserting equality on the
     * comparable record fields. SDK-generated {@code equals} on {@code AttributeValue} maps
     * does the heavy lifting (recursive for {@code L}/{@code M}, byte-content for {@code B}).
     *
     * <p>Skipped intentionally: {@code eventID} (independent UUIDs per backend),
     * {@code sequenceNumber} (backend-specific spaces), {@code approximateCreationDateTime}
     * (wall-clock drift), {@code awsRegion} (each side configures its own region).
     * {@code sizeBytes} is checked as {@code > 0} only — each backend computes it
     * independently.
     */
    private static void assertRecordSequenceEquals(String ctx, List<Record> phoenix,
                                                   List<Record> ddb) {
        Assert.assertEquals("Parity (" + ctx + ") size mismatch", ddb.size(), phoenix.size());
        for (int i = 0; i < ddb.size(); i++) {
            Record p = phoenix.get(i);
            Record d = ddb.get(i);
            Assert.assertEquals("Parity (" + ctx + ") eventName at " + i,
                    d.getEventName(), p.getEventName());
            Assert.assertEquals("Parity (" + ctx + ") eventVersion at " + i,
                    d.getEventVersion(), p.getEventVersion());
            Assert.assertEquals("Parity (" + ctx + ") eventSource at " + i,
                    d.getEventSource(), p.getEventSource());
            Assert.assertEquals("Parity (" + ctx + ") streamViewType at " + i,
                    d.getDynamodb().getStreamViewType(), p.getDynamodb().getStreamViewType());
            Assert.assertEquals("Parity (" + ctx + ") keys at " + i,
                    d.getDynamodb().getKeys(), p.getDynamodb().getKeys());
            Assert.assertEquals("Parity (" + ctx + ") newImage at " + i,
                    d.getDynamodb().getNewImage(), p.getDynamodb().getNewImage());
            Assert.assertEquals("Parity (" + ctx + ") oldImage at " + i,
                    d.getDynamodb().getOldImage(), p.getDynamodb().getOldImage());
            assertSizeBytesPositive(ctx, "phoenix", i, p);
            assertSizeBytesPositive(ctx, "ddb", i, d);
        }
    }

    private static void assertSizeBytesPositive(String ctx, String side, int i, Record r) {
        Long sizeBytes = r.getDynamodb().getSizeBytes();
        Assert.assertTrue("Parity (" + ctx + ") " + side + " sizeBytes not > 0 at " + i
                        + " (got " + sizeBytes + ")",
                sizeBytes != null && sizeBytes > 0);
    }

    private static Map<String, List<Record>> groupByPk1(List<Record> records) {
        Map<String, List<Record>> out = new LinkedHashMap<>();
        for (Record r : records) {
            AttributeValue pk1Av = r.getDynamodb().getKeys().get("PK1");
            String pk1 = pk1Av != null && pk1Av.getS() != null ? pk1Av.getS() : "";
            out.computeIfAbsent(pk1, k -> new ArrayList<>()).add(r);
        }
        return out;
    }
}
