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
import org.apache.phoenix.ddb.TestUtils;
import org.apache.phoenix.ddb.utils.PhoenixUtils;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ManualEnvironmentEdge;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.TimeToLiveSpecification;
import software.amazon.awssdk.services.dynamodb.model.UpdateTimeToLiveRequest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Verifies the Worker survives "records trimmed beneath an open iterator": records on a
 * shard are TTL-deleted (clock advance + major compaction) while KCL is actively polling,
 * then new writes arrive and must still be delivered. AWS's {@code ExpiredIteratorException}
 * never fires here because {@code PhoenixShardIterator} is stateless and never expires.
 *
 * <p>Phoenix-only by design — no {@link DualOracle}: TTL prunes on HBase major compaction
 * via an injected {@link ManualEnvironmentEdge} on the Phoenix side and against real wall
 * clock on the DDB side, so the "trimmed under an open iterator" sets cannot match.
 */
public class KclTtlExpiryIT extends KclTestBase {

    @After
    public void resetEdgeManager() {
        EnvironmentEdgeManager.reset();
    }

    @Test(timeout = 180_000)
    public void workerSurvivesRecordTrimming() throws Exception {
        String tableName = "ttlIT_" + System.currentTimeMillis();
        CreateTableRequest create = DDLTestUtils.addStreamSpecToRequest(
                DDLTestUtils.getCreateTableRequest(tableName, "PK1", ScalarAttributeType.S,
                        "PK2", ScalarAttributeType.N),
                "NEW_AND_OLD_IMAGES");
        phoenixV2.createTable(create);
        phoenixV2.updateTimeToLive(UpdateTimeToLiveRequest.builder()
                .tableName(tableName)
                .timeToLiveSpecification(TimeToLiveSpecification.builder()
                        .attributeName("ttlAttr").enabled(true).build())
                .build());

        String streamArn = KclTestUtils.streamArnFor(phoenixStreamsV2, tableName);

        // Item with TTL 4h in the future.
        long expiry = (System.currentTimeMillis() + TimeUnit.HOURS.toMillis(4)) / 1000;
        putWithTtl(tableName, "K", 1, "before-trim-1", expiry);
        putWithTtl(tableName, "K", 2, "before-trim-2", expiry);

        String appName = KclTestUtils.uniqueApplicationName("ttlApp");
        lifecycle.trackAppName(appName);

        RecordingStore store = new RecordingStore();
        KinesisClientLibConfiguration config = kclConfig(appName, streamArn);
        lifecycle.track(startWorker(config,
                new RecordingRecordProcessorFactory(store, RecordingRecordProcessor.Behavior.defaults())));

        KclTestUtils.awaitConsumerCaughtUp(store, 2, Duration.ofSeconds(45));

        // Advance clock 28h past max lookback + TTL window, then major compact to trim.
        ManualEnvironmentEdge edge = new ManualEnvironmentEdge();
        long t = System.currentTimeMillis() + TimeUnit.HOURS.toMillis(28);
        t = (t / 1000) * 1000;
        EnvironmentEdgeManager.injectEdge(edge);
        edge.setValue(t);
        try (Connection conn = DriverManager.getConnection(jdbcUrl())) {
            TestUtil.doMajorCompaction(conn, SchemaUtil.getEscapedArgument(
                    PhoenixUtils.getFullTableName(tableName, false)));
        }

        // After trim, write new records — the Worker must still be alive and deliver them.
        KclTestUtils.putRow(phoenixV2, tableName, "K", 3, "post-trim-3");
        KclTestUtils.putRow(phoenixV2, tableName, "K", 4, "post-trim-4");

        KclTestUtils.awaitConsumerCaughtUp(store, 4, Duration.ofSeconds(60));
    }

    private void putWithTtl(String tableName, String pk1, int pk2, String payload, long ttlSeconds) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("PK1", AttributeValue.builder().s(pk1).build());
        item.put("PK2", AttributeValue.builder().n(Integer.toString(pk2)).build());
        item.put("payload", AttributeValue.builder().s(payload).build());
        item.put("ttlAttr", AttributeValue.builder().n(Long.toString(ttlSeconds)).build());
        phoenixV2.putItem(PutItemRequest.builder().tableName(tableName).item(item).build());
    }
}
