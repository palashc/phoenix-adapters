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

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import org.apache.phoenix.ddb.LocalDynamoDB;
import org.apache.phoenix.ddb.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;

import java.time.Duration;
import java.util.concurrent.Executors;

/**
 * Owns a KCL IT's table + stream + writes across both backends: {@code phoenix-ddb-rest}
 * (system under test, {@code phoenix*}) and {@link LocalDynamoDB} (AWS-shape reference,
 * {@code ddb*}). Creates the table on both, waits for both streams to reach {@code ENABLED},
 * starts a KCL Worker against the DDB stream as the reference recorder, and dual-writes
 * every mutation. Divergence between {@code store()} (the test's Phoenix-side worker) and
 * {@link #ddbStore()} is a {@code phoenix-ddb-rest} bug.
 *
 * <p>Register with {@code lifecycle.track(oracle)} for {@code @After} cleanup. Mutations
 * default to {@link WriteMode#BOTH}; pass {@link WriteMode#PHOENIX_ONLY} when a test's
 * semantics are Phoenix-specific (e.g. TTL).
 *
 * <p>DDB Streams on {@link LocalDynamoDB} produces one shard per stream and never splits;
 * tests that split on the Phoenix side must compare via
 * {@link KclTestUtils.ParityMode#PER_KEY_SEQUENCE} (topology differs, per-key ordering
 * does not).
 */
public final class DualOracle implements AutoCloseable {

    /** Per-call switch controlling dual-write fan-out. */
    public enum WriteMode {
        /** Default — Phoenix-first, then mirror to DDB. */
        BOTH,
        /** Skip the DDB mirror; used by tests with Phoenix-specific semantics. */
        PHOENIX_ONLY
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(DualOracle.class);
    private static final Duration WORKER_READY_TIMEOUT = Duration.ofSeconds(60);
    private static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(15);

    private final DynamoDbClient phoenixV2;
    private final DynamoDbClient ddbV2;
    private final AmazonDynamoDB ddbV1;
    private final RecordingStore ddbStore;
    private final String tableName;
    private final String phoenixStreamArn;
    private final String ddbAppName;
    private final KclTestBase.WorkerHandle worker;

    private DualOracle(DynamoDbClient phoenixV2, DynamoDbClient ddbV2, AmazonDynamoDB ddbV1,
                       RecordingStore ddbStore, String tableName, String phoenixStreamArn,
                       String ddbAppName, KclTestBase.WorkerHandle worker) {
        this.phoenixV2 = phoenixV2;
        this.ddbV2 = ddbV2;
        this.ddbV1 = ddbV1;
        this.ddbStore = ddbStore;
        this.tableName = tableName;
        this.phoenixStreamArn = phoenixStreamArn;
        this.ddbAppName = ddbAppName;
        this.worker = worker;
    }

    /**
     * Create the table on both backends from a single {@link CreateTableRequest}, wait for
     * both streams to reach {@code ENABLED}, and spin up a KCL Worker against the DDB-side
     * stream to record the reference view of what was written.
     */
    public static DualOracle create(DynamoDbClient phoenixV2,
                                    DynamoDbStreamsClient phoenixStreamsV2,
                                    LocalDynamoDB localDdb,
                                    CreateTableRequest createRequest) throws InterruptedException {
        String tableName = createRequest.tableName();

        // Phoenix side: create table, resolve stream ARN, wait for ENABLED.
        phoenixV2.createTable(createRequest);
        String phoenixStreamArn = KclTestUtils.streamArnFor(phoenixStreamsV2, tableName);
        TestUtils.waitForStream(phoenixStreamsV2, phoenixStreamArn);

        // DDB side: create table, resolve stream ARN, wait for ENABLED.
        DynamoDbClient ddbV2 = localDdb.createV2Client();
        DynamoDbStreamsClient ddbStreamsV2 = localDdb.createV2StreamsClient();
        ddbV2.createTable(createRequest);
        String ddbStreamArn = KclTestUtils.streamArnFor(ddbStreamsV2, tableName);
        TestUtils.waitForStream(ddbStreamsV2, ddbStreamArn);
        try { ddbStreamsV2.close(); } catch (RuntimeException ignored) { /* best effort */ }

        AmazonDynamoDB ddbV1 = KclTestBase.newDdbV1Client(localDdb);
        AmazonDynamoDBStreamsAdapterClient ddbAdapter = KclTestBase.newDdbAdapterClient(localDdb);
        AmazonCloudWatch cw = KclTestBase.newMockCloudWatch();

        String ddbAppName = KclTestUtils.uniqueApplicationName("oracle_" + tableName);
        KinesisClientLibConfiguration config = KclTestBase.kclConfig(ddbAppName, ddbStreamArn)
                .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON);

        RecordingStore ddbStore = new RecordingStore();
        KclTestBase.WorkerHandle worker = KclTestBase.startWorkerStatic(
                config,
                new RecordingRecordProcessorFactory(ddbStore,
                        RecordingRecordProcessor.Behavior.defaults()),
                ddbAdapter, ddbV1, cw, Executors.newCachedThreadPool());

        if (!KclTestUtils.awaitWorkerReady(ddbV1, ddbAppName, WORKER_READY_TIMEOUT)) {
            LOGGER.warn("DualOracle DDB worker did not claim a lease within {}",
                    WORKER_READY_TIMEOUT);
        }
        return new DualOracle(phoenixV2, ddbV2, ddbV1, ddbStore, tableName, phoenixStreamArn,
                ddbAppName, worker);
    }

    public String tableName() {
        return tableName;
    }

    public String phoenixStreamArn() {
        return phoenixStreamArn;
    }

    /** DDB-side records observed by the reference KCL Worker. */
    public RecordingStore ddbStore() {
        return ddbStore;
    }

    // -------------------------------------------------- write helpers

    /** Standard {@code (PK1=S, PK2=N, payload=S)} dual-write. */
    public void putRow(String pk1, int pk2, String payload) {
        putRow(pk1, pk2, payload, WriteMode.BOTH);
    }

    public void putRow(String pk1, int pk2, String payload, WriteMode mode) {
        KclTestUtils.putRow(phoenixV2, tableName, pk1, pk2, payload);
        if (mode == WriteMode.BOTH) {
            try {
                KclTestUtils.putRow(ddbV2, tableName, pk1, pk2, payload);
            } catch (RuntimeException e) {
                throw new AssertionError("DualOracle mirror putRow failed on DDB", e);
            }
        }
    }

    public void putItem(PutItemRequest request) {
        putItem(request, WriteMode.BOTH);
    }

    public void putItem(PutItemRequest request, WriteMode mode) {
        phoenixV2.putItem(request);
        if (mode == WriteMode.BOTH) {
            try {
                ddbV2.putItem(request);
            } catch (RuntimeException e) {
                throw new AssertionError("DualOracle mirror putItem failed on DDB", e);
            }
        }
    }

    public void updateItem(UpdateItemRequest request) {
        updateItem(request, WriteMode.BOTH);
    }

    public void updateItem(UpdateItemRequest request, WriteMode mode) {
        phoenixV2.updateItem(request);
        if (mode == WriteMode.BOTH) {
            try {
                ddbV2.updateItem(request);
            } catch (RuntimeException e) {
                throw new AssertionError("DualOracle mirror updateItem failed on DDB", e);
            }
        }
    }

    public void deleteItem(DeleteItemRequest request) {
        deleteItem(request, WriteMode.BOTH);
    }

    public void deleteItem(DeleteItemRequest request, WriteMode mode) {
        phoenixV2.deleteItem(request);
        if (mode == WriteMode.BOTH) {
            try {
                ddbV2.deleteItem(request);
            } catch (RuntimeException e) {
                throw new AssertionError("DualOracle mirror deleteItem failed on DDB", e);
            }
        }
    }

    @Override
    public void close() {
        if (!worker.isDone()) {
            worker.gracefulShutdown(SHUTDOWN_TIMEOUT);
        }
        try {
            ddbV1.deleteTable(ddbAppName);
        } catch (com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException ignored) {
            /* lease table never created */
        } catch (RuntimeException e) {
            LOGGER.warn("Failed to delete oracle lease table {}: {}", ddbAppName, e.toString());
        }
        try {
            ddbV2.deleteTable(DeleteTableRequest.builder().tableName(tableName).build());
        } catch (ResourceNotFoundException ignored) {
            /* user table never created */
        } catch (RuntimeException e) {
            LOGGER.warn("Failed to delete oracle user table {}: {}", tableName, e.toString());
        }
        try { ddbV1.shutdown(); } catch (RuntimeException ignored) { /* best effort */ }
        try { ddbV2.close(); } catch (RuntimeException ignored) { /* best effort */ }
    }
}
