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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Parameterized across all four DynamoDB Stream {@code StreamViewType} settings — runs the
 * same INSERT → MODIFY → REMOVE sequence under each view type and asserts the image shape
 * KCL delivers matches the AWS spec for that view:
 *
 * <table border="1">
 *   <tr><th>View type</th><th>INSERT image</th><th>MODIFY image</th><th>REMOVE image</th></tr>
 *   <tr><td>KEYS_ONLY</td>          <td>keys only</td>           <td>keys only</td>           <td>keys only</td></tr>
 *   <tr><td>NEW_IMAGE</td>          <td>newImage</td>            <td>newImage</td>            <td>(no images)</td></tr>
 *   <tr><td>OLD_IMAGE</td>          <td>(no images)</td>         <td>oldImage</td>            <td>oldImage</td></tr>
 *   <tr><td>NEW_AND_OLD_IMAGES</td> <td>newImage</td>            <td>old + new</td>           <td>oldImage</td></tr>
 * </table>
 *
 * <p>Every other IT in the suite uses {@code NEW_AND_OLD_IMAGES} exclusively. A regression
 * that, say, populated {@code newImage} on REMOVE events under {@code NEW_IMAGE} view, or
 * silently dropped keys when configured for {@code KEYS_ONLY}, would not be caught
 * elsewhere.
 */
@RunWith(Parameterized.class)
public class KclStreamViewTypesIT extends KclTestBase {

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> viewTypes() {
        return Arrays.asList(new Object[][]{
                {"KEYS_ONLY"},
                {"NEW_IMAGE"},
                {"OLD_IMAGE"},
                {"NEW_AND_OLD_IMAGES"},
        });
    }

    private final String streamViewType;

    public KclStreamViewTypesIT(String streamViewType) {
        this.streamViewType = streamViewType;
    }

    @Test(timeout = 120_000)
    public void imageShapeMatchesStreamViewType() throws Exception {
        String tableName = "viewType_" + streamViewType.toLowerCase()
                + "_" + System.currentTimeMillis();
        CreateTableRequest create = DDLTestUtils.addStreamSpecToRequest(
                DDLTestUtils.getCreateTableRequest(tableName, "PK1", ScalarAttributeType.S,
                        "PK2", ScalarAttributeType.N),
                streamViewType);
        DualOracle oracle = lifecycle.track(DualOracle.create(
                phoenixV2, phoenixStreamsV2, localDdb(), create));
        String streamArn = oracle.phoenixStreamArn();

        String appName = KclTestUtils.uniqueApplicationName("viewType_" + streamViewType);
        lifecycle.trackAppName(appName);
        RecordingStore store = new RecordingStore();
        KinesisClientLibConfiguration config = kclConfig(appName, streamArn);
        lifecycle.track(startWorker(config,
                new RecordingRecordProcessorFactory(store, RecordingRecordProcessor.Behavior.defaults())));

        // INSERT, MODIFY, REMOVE for one key.
        putRow(oracle, tableName, "K", 1, "payload-v1");
        updateRow(oracle, tableName, "K", 1, "payload-v2");
        deleteRow(oracle, tableName, "K", 1);

        KclTestUtils.awaitConsumerCaughtUp(store, 3, Duration.ofSeconds(60));

        List<com.amazonaws.services.dynamodbv2.model.Record> records = store.allRecordsInArrival();
        Assert.assertEquals(3, records.size());
        Assert.assertEquals("INSERT", records.get(0).getEventName());
        Assert.assertEquals("MODIFY", records.get(1).getEventName());
        Assert.assertEquals("REMOVE", records.get(2).getEventName());

        // Keys must always be present regardless of view type.
        for (com.amazonaws.services.dynamodbv2.model.Record r : records) {
            Map<String, com.amazonaws.services.dynamodbv2.model.AttributeValue> keys =
                    r.getDynamodb().getKeys();
            Assert.assertNotNull("Keys must always be present (view=" + streamViewType + ")", keys);
            Assert.assertEquals("K", keys.get("PK1").getS());
            Assert.assertEquals("1", keys.get("PK2").getN());
        }

        com.amazonaws.services.dynamodbv2.model.Record insert = records.get(0);
        com.amazonaws.services.dynamodbv2.model.Record modify = records.get(1);
        com.amazonaws.services.dynamodbv2.model.Record remove = records.get(2);

        switch (streamViewType) {
            case "KEYS_ONLY":
                assertNoImages(insert, "INSERT under KEYS_ONLY");
                assertNoImages(modify, "MODIFY under KEYS_ONLY");
                assertNoImages(remove, "REMOVE under KEYS_ONLY");
                break;
            case "NEW_IMAGE":
                assertOnlyNew(insert, "INSERT under NEW_IMAGE", "payload-v1");
                assertOnlyNew(modify, "MODIFY under NEW_IMAGE", "payload-v2");
                assertNoImages(remove, "REMOVE under NEW_IMAGE");
                break;
            case "OLD_IMAGE":
                assertNoImages(insert, "INSERT under OLD_IMAGE");
                assertOnlyOld(modify, "MODIFY under OLD_IMAGE", "payload-v1");
                assertOnlyOld(remove, "REMOVE under OLD_IMAGE", "payload-v2");
                break;
            case "NEW_AND_OLD_IMAGES":
                assertOnlyNew(insert, "INSERT under NEW_AND_OLD_IMAGES", "payload-v1");
                Assert.assertNotNull("MODIFY newImage missing", modify.getDynamodb().getNewImage());
                Assert.assertNotNull("MODIFY oldImage missing", modify.getDynamodb().getOldImage());
                Assert.assertEquals("payload-v2",
                        modify.getDynamodb().getNewImage().get("payload").getS());
                Assert.assertEquals("payload-v1",
                        modify.getDynamodb().getOldImage().get("payload").getS());
                assertOnlyOld(remove, "REMOVE under NEW_AND_OLD_IMAGES", "payload-v2");
                break;
            default:
                Assert.fail("Unhandled view type: " + streamViewType);
        }

        // DDB parity: STRICT_TOTAL_ORDER — exactly 3 records on each side (one shard, one
        // key, deterministic INSERT → MODIFY → REMOVE). Catches any per-view-type image
        // shape divergence between phoenix-ddb-rest's CDC and the AWS reference.
        KclTestUtils.awaitConsumerCaughtUp(oracle.ddbStore(), 3, Duration.ofSeconds(30));
        KclTestUtils.assertContentParity(store, oracle.ddbStore(),
                KclTestUtils.ParityMode.STRICT_TOTAL_ORDER);
    }

    private static void assertNoImages(com.amazonaws.services.dynamodbv2.model.Record r, String ctx) {
        Assert.assertNull(ctx + ": expected no newImage", r.getDynamodb().getNewImage());
        Assert.assertNull(ctx + ": expected no oldImage", r.getDynamodb().getOldImage());
    }

    private static void assertOnlyNew(com.amazonaws.services.dynamodbv2.model.Record r,
                                      String ctx, String expectedPayload) {
        Assert.assertNotNull(ctx + ": newImage required", r.getDynamodb().getNewImage());
        Assert.assertNull(ctx + ": oldImage must be absent", r.getDynamodb().getOldImage());
        Assert.assertEquals(ctx + ": newImage payload mismatch", expectedPayload,
                r.getDynamodb().getNewImage().get("payload").getS());
    }

    private static void assertOnlyOld(com.amazonaws.services.dynamodbv2.model.Record r,
                                      String ctx, String expectedPayload) {
        Assert.assertNotNull(ctx + ": oldImage required", r.getDynamodb().getOldImage());
        Assert.assertNull(ctx + ": newImage must be absent", r.getDynamodb().getNewImage());
        Assert.assertEquals(ctx + ": oldImage payload mismatch", expectedPayload,
                r.getDynamodb().getOldImage().get("payload").getS());
    }

    private static void putRow(DualOracle oracle, String tableName, String pk1, int pk2,
                               String payload) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("PK1", AttributeValue.builder().s(pk1).build());
        item.put("PK2", AttributeValue.builder().n(Integer.toString(pk2)).build());
        item.put("payload", AttributeValue.builder().s(payload).build());
        oracle.putItem(PutItemRequest.builder().tableName(tableName).item(item).build());
    }

    private static void updateRow(DualOracle oracle, String tableName, String pk1, int pk2,
                                  String payload) {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("PK1", AttributeValue.builder().s(pk1).build());
        key.put("PK2", AttributeValue.builder().n(Integer.toString(pk2)).build());
        Map<String, String> exprNames = new HashMap<>();
        exprNames.put("#p", "payload");
        Map<String, AttributeValue> exprVals = new HashMap<>();
        exprVals.put(":p", AttributeValue.builder().s(payload).build());
        oracle.updateItem(UpdateItemRequest.builder()
                .tableName(tableName).key(key)
                .updateExpression("SET #p = :p")
                .expressionAttributeNames(exprNames)
                .expressionAttributeValues(exprVals)
                .build());
    }

    private static void deleteRow(DualOracle oracle, String tableName, String pk1, int pk2) {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("PK1", AttributeValue.builder().s(pk1).build());
        key.put("PK2", AttributeValue.builder().n(Integer.toString(pk2)).build());
        oracle.deleteItem(DeleteItemRequest.builder().tableName(tableName).key(key).build());
    }
}
