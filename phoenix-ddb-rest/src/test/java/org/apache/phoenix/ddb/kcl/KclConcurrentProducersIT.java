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
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Exercises sustained concurrent producer load against a single KCL consumer.
 *
 * <p>Eight producer threads each own one disjoint partition key ({@code P0..P7}) and write
 * {@code WRITES_PER_PRODUCER} records at maximum speed. After all producers finish, a
 * single KCL Worker reads from {@code TRIM_HORIZON} and must deliver every record. Per-key
 * write order is asserted (records on a single partition key route to a single shard, so
 * KCL's per-shard ordering guarantee promises in-order delivery per key).
 *
 * <p>Cross-key order is intentionally NOT asserted — concurrent producers have no global
 * ordering, and KCL doesn't promise one either.
 */
public class KclConcurrentProducersIT extends KclTestBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(KclConcurrentProducersIT.class);

    private static final int NUM_PRODUCERS = 8;
    private static final int WRITES_PER_PRODUCER = 50;
    private static final int TOTAL_RECORDS = NUM_PRODUCERS * WRITES_PER_PRODUCER; // 400

    @Test(timeout = 240_000)
    public void perKeyOrderPreservedUnderConcurrentProducers() throws Exception {
        String tableName = "concurrentProducersIT_" + System.currentTimeMillis();
        CreateTableRequest create = DDLTestUtils.addStreamSpecToRequest(
                DDLTestUtils.getCreateTableRequest(tableName, "PK1", ScalarAttributeType.S,
                        "PK2", ScalarAttributeType.N),
                "NEW_AND_OLD_IMAGES");
        DualOracle oracle = lifecycle.track(DualOracle.create(
                phoenixV2, phoenixStreamsV2, localDdb(), create));
        String streamArn = oracle.phoenixStreamArn();

        // Drive concurrent writes BEFORE starting the consumer, so CDC index emits records
        // under producer contention rather than serialized by consumer pacing.
        ExecutorService producerPool = Executors.newFixedThreadPool(NUM_PRODUCERS);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(NUM_PRODUCERS);
        AtomicReference<Throwable> firstError = new AtomicReference<>();

        for (int producerIdx = 0; producerIdx < NUM_PRODUCERS; producerIdx++) {
            final String pk1 = "P" + producerIdx;
            producerPool.submit(() -> {
                try {
                    start.await();
                    for (int seq = 0; seq < WRITES_PER_PRODUCER; seq++) {
                        oracle.putRow(pk1, seq, "v-" + seq);
                    }
                } catch (Throwable t) {
                    firstError.compareAndSet(null, t);
                } finally {
                    done.countDown();
                }
            });
        }

        long writeStartNanos = System.nanoTime();
        start.countDown();
        Assert.assertTrue("Producers did not finish in time",
                done.await(120, TimeUnit.SECONDS));
        producerPool.shutdown();
        long writeMs = (System.nanoTime() - writeStartNanos) / 1_000_000L;
        LOGGER.info("Producers wrote {} records in {} ms ({} writes/sec)",
                TOTAL_RECORDS, writeMs, (TOTAL_RECORDS * 1000L) / Math.max(writeMs, 1));
        Assert.assertNull("A producer thread failed", firstError.get());

        // Start consumer from TRIM_HORIZON; must see every record.
        String appName = KclTestUtils.uniqueApplicationName("concurrentProducersApp");
        lifecycle.trackAppName(appName);
        RecordingStore store = new RecordingStore();
        KinesisClientLibConfiguration config = kclConfig(appName, streamArn).withMaxRecords(50);
        lifecycle.track(startWorker(config,
                new RecordingRecordProcessorFactory(store, RecordingRecordProcessor.Behavior.defaults())));

        KclTestUtils.awaitConsumerCaughtUp(store, TOTAL_RECORDS, Duration.ofSeconds(120));

        // Per-key ordering: each producer's PK2 sequence (0..WRITES_PER_PRODUCER-1) must
        // arrive in order. Since each producer owns one partition key and a partition key
        // hashes to a single shard, KCL's per-shard ordering guarantee yields this.
        for (int producerIdx = 0; producerIdx < NUM_PRODUCERS; producerIdx++) {
            String pk1 = "P" + producerIdx;
            List<Integer> seen = KclTestUtils.pk2SequenceForPk1(store, pk1);
            List<Integer> expected = IntStream.range(0, WRITES_PER_PRODUCER)
                    .boxed().collect(Collectors.toList());
            Assert.assertEquals("Per-key order broken for " + pk1, expected, seen);
        }

        LOGGER.info("Consumer received {} records across {} shard(s); per-shard counts: {}",
                store.totalCount(), store.shardsSeen().size(), store.countByShard());

        // DDB parity: PER_KEY_SEQUENCE under concurrent producer contention. KCL's per-shard
        // ordering guarantee + single partition key → single shard means the 8 per-key
        // sequences must match exactly on both sides.
        KclTestUtils.awaitConsumerCaughtUp(oracle.ddbStore(), TOTAL_RECORDS, Duration.ofSeconds(60));
        KclTestUtils.assertContentParity(store, oracle.ddbStore(),
                KclTestUtils.ParityMode.PER_KEY_SEQUENCE);
    }
}
