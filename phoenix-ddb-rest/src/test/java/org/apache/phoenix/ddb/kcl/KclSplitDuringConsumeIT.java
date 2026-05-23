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
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Verifies KCL's parent-before-child contract end-to-end: when a split fires while a slow
 * Worker is mid-consumption, every parent-shard record is delivered (and the parent's lease
 * reaches {@link KclTestUtils#SHARD_END}) before any daughter-shard record is delivered.
 *
 * <p>Uses a producer thread for steady write pressure, {@code MaxRecords(1)} for fine-grained
 * batches, and a small {@code sleepBeforeBatch} on the processor to keep the consumer behind
 * the writer so the split lands mid-stream.
 */
public class KclSplitDuringConsumeIT extends KclTestBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(KclSplitDuringConsumeIT.class);

    @Test(timeout = 240_000)
    public void parentDrainsBeforeDaughtersOpen() throws Exception {
        String tableName = "splitMidIT_" + System.currentTimeMillis();
        CreateTableRequest create = DDLTestUtils.addStreamSpecToRequest(
                DDLTestUtils.getCreateTableRequest(tableName, "PK1", ScalarAttributeType.S,
                        "PK2", ScalarAttributeType.N),
                "NEW_AND_OLD_IMAGES");
        DualOracle oracle = lifecycle.track(DualOracle.create(
                phoenixV2, phoenixStreamsV2, localDdb(), create));
        String streamArn = oracle.phoenixStreamArn();

        String appName = KclTestUtils.uniqueApplicationName("splitMidApp");
        lifecycle.trackAppName(appName);

        RecordingStore store = new RecordingStore();
        RecordingRecordProcessor.Behavior behavior = RecordingRecordProcessor.Behavior.builder()
                .sleepBeforeBatch(Duration.ofMillis(200))
                .build();
        KinesisClientLibConfiguration config = kclConfig(appName, streamArn)
                .withIdleTimeBetweenReadsInMillis(100)
                .withMaxRecords(1);

        AtomicBoolean stopProducer = new AtomicBoolean(false);
        Thread producer = new Thread(() -> {
            int i = 0;
            while (!stopProducer.get()) {
                String pk1 = (i % 2 == 0) ? "ABC" : "XYZ"; // straddle the planned split "LMN"
                try {
                    oracle.putRow(pk1, i, "p-" + i);
                } catch (RuntimeException e) {
                    LOGGER.warn("Producer write failed at {}: {}", i, e.toString());
                }
                i++;
                KclTestUtils.sleepQuietly(Duration.ofMillis(50));
            }
        }, "splitMid-producer");
        producer.start();

        try {
            lifecycle.track(startWorker(config,
                    new RecordingRecordProcessorFactory(store, behavior)));

            // Wait until the Worker is actually consuming (liveness check while producer
            // is still writing — quiescence won't fire here).
            Assert.assertTrue("Worker never produced any record",
                    KclTestUtils.awaitTrue(() -> store.totalCount() >= 3,
                            Duration.ofSeconds(30)));

            // Fire the split while writes continue.
            KclTestUtils.splitTableAt(jdbcUrl(), tableName, "LMN");

            // Let the system drain past the split.
            KclTestUtils.sleepQuietly(Duration.ofSeconds(10));
        } finally {
            stopProducer.set(true);
            producer.join(5000);
        }

        // Wait for the lease table to reflect at least 3 shards (parent + 2 daughters).
        // KCL only opens daughter leases after the parent's lease checkpoint reaches
        // SHARD_END; the parent must finish delivering its in-flight buffer first.
        // With a slow consumer + producer pressure, that can take well over 30s.
        Map<String, Map<String, AttributeValue>> leases =
                KclTestUtils.awaitLeaseCount(phoenixV1, appName, 3, Duration.ofSeconds(90));
        Assert.assertTrue("Expected at least 3 leases (parent + 2 daughters), got: " + leases.keySet(),
                leases.size() >= 3);

        // Identify parent vs daughters by checkpoint: parent has SHARD_END.
        String parentShard = null;
        for (Map.Entry<String, Map<String, AttributeValue>> e : leases.entrySet()) {
            AttributeValue ck = e.getValue().get(KclTestUtils.CHECKPOINT_COLUMN);
            if (ck != null && KclTestUtils.SHARD_END.equals(ck.getS())) {
                parentShard = e.getKey();
                break;
            }
        }
        Assert.assertNotNull("No SHARD_END lease found — parent never drained: " + leases.keySet(),
                parentShard);

        // Parent-before-child: the parent shard's last delivered record's arrival index
        // must be less than the first delivered record's arrival index for every daughter.
        int parentLastArrival = store.lastArrivalIndexForShard(parentShard);
        Assert.assertTrue("Parent shard never produced records (no in-flight before split?)",
                parentLastArrival >= 0);

        for (String shardId : store.shardsSeen()) {
            if (shardId.equals(parentShard)) {
                continue;
            }
            int firstDaughterArrival = store.firstArrivalIndexForShard(shardId);
            Assert.assertTrue(
                    "Daughter " + shardId + " (firstArrival=" + firstDaughterArrival
                            + ") delivered before parent " + parentShard
                            + " (lastArrival=" + parentLastArrival + ") finished",
                    firstDaughterArrival > parentLastArrival);
        }

        LOGGER.info("Per-shard counts: {}", store.countByShard());

        // DDB parity: wait for both consumers to go quiescent independently — neither side
        // knows the producer's count, they just consume until the stream goes quiet. Phoenix
        // is slowed by sleepBeforeBatch(200ms) and reaches quiescence later than the oracle's
        // default-speed worker. PER_KEY_SEQUENCE survives the Phoenix-side split topology
        // (DDB stays single-shard).
        int phoenixCount = KclTestUtils.awaitQuiescence(store, Duration.ofSeconds(120));
        int ddbCount = KclTestUtils.awaitQuiescence(oracle.ddbStore(), Duration.ofSeconds(60));
        LOGGER.info("Quiesced: phoenix={} ddb={}", phoenixCount, ddbCount);
        KclTestUtils.assertContentParity(store, oracle.ddbStore(),
                KclTestUtils.ParityMode.PER_KEY_SEQUENCE);
    }
}
