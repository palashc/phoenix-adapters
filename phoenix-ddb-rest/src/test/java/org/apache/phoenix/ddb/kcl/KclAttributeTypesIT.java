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
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Exercises every DynamoDB attribute type through the full CDC + KCL pipeline. The other
 * ITs all write {@code S}/{@code N}-only items, which means a regression in the CDC
 * serializer for {@code B}, {@code BOOL}, {@code L}, {@code M}, {@code SS}, {@code NS},
 * {@code BS}, or {@code NULL} would silently corrupt customer data without surfacing here.
 *
 * <p>Concrete concern: consumers commonly encode their in-record payload as a protobuf
 * {@code byte[]} stored in a {@code B} attribute. A regression that hex-encodes (or worse,
 * drops) binary attributes during stream emission would not be caught by any other test
 * in this suite.
 *
 * <p>This test writes one item containing every supported attribute type, then asserts the
 * KCL consumer sees {@code newImage} byte-equal to the original on the INSERT event.
 */
public class KclAttributeTypesIT extends KclTestBase {

    @Test(timeout = 120_000)
    public void allDdbAttributeTypesRoundTripThroughCdc() throws Exception {
        String tableName = "attrTypesIT_" + System.currentTimeMillis();
        CreateTableRequest create = DDLTestUtils.addStreamSpecToRequest(
                DDLTestUtils.getCreateTableRequest(tableName, "PK1", ScalarAttributeType.S,
                        "PK2", ScalarAttributeType.N),
                "NEW_AND_OLD_IMAGES");
        DualOracle oracle = lifecycle.track(DualOracle.create(
                phoenixV2, phoenixStreamsV2, localDdb(), create));
        String streamArn = oracle.phoenixStreamArn();

        String appName = KclTestUtils.uniqueApplicationName("attrTypesApp");
        lifecycle.trackAppName(appName);
        RecordingStore store = new RecordingStore();
        KinesisClientLibConfiguration config = kclConfig(appName, streamArn);
        lifecycle.track(startWorker(config,
                new RecordingRecordProcessorFactory(store, RecordingRecordProcessor.Behavior.defaults())));

        // Build an item touching every DDB scalar, set, and document attribute type.
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("PK1", AttributeValue.builder().s("ATTR").build());
        item.put("PK2", AttributeValue.builder().n("1").build());
        item.put("attrS", AttributeValue.builder().s("simple string").build());
        item.put("attrN", AttributeValue.builder().n("3.14159").build());
        item.put("attrB", AttributeValue.builder()
                .b(SdkBytes.fromByteArray(new byte[]{ 0x00, 0x01, (byte) 0xFE, (byte) 0xFF }))
                .build());
        item.put("attrBoolTrue", AttributeValue.builder().bool(true).build());
        item.put("attrBoolFalse", AttributeValue.builder().bool(false).build());
        item.put("attrNull", AttributeValue.builder().nul(true).build());
        item.put("attrSS", AttributeValue.builder()
                .ss(Arrays.asList("a", "b", "c")).build());
        item.put("attrNS", AttributeValue.builder()
                .ns(Arrays.asList("1", "2", "3")).build());
        item.put("attrBS", AttributeValue.builder()
                .bs(Arrays.asList(
                        SdkBytes.fromByteArray("x".getBytes(StandardCharsets.UTF_8)),
                        SdkBytes.fromByteArray("y".getBytes(StandardCharsets.UTF_8))))
                .build());
        item.put("attrL", AttributeValue.builder()
                .l(Arrays.asList(
                        AttributeValue.builder().s("a-string-in-a-list").build(),
                        AttributeValue.builder().n("42").build(),
                        AttributeValue.builder().bool(true).build()))
                .build());
        Map<String, AttributeValue> nested = new HashMap<>();
        nested.put("nestedStr", AttributeValue.builder().s("hello").build());
        nested.put("nestedNum", AttributeValue.builder().n("99").build());
        nested.put("nestedBool", AttributeValue.builder().bool(true).build());
        item.put("attrM", AttributeValue.builder().m(nested).build());

        oracle.putItem(PutItemRequest.builder().tableName(tableName).item(item).build());

        KclTestUtils.awaitConsumerCaughtUp(store, 1, Duration.ofSeconds(60));

        com.amazonaws.services.dynamodbv2.model.Record rec = store.allRecordsInArrival().get(0);
        Assert.assertEquals("INSERT", rec.getEventName());

        Map<String, com.amazonaws.services.dynamodbv2.model.AttributeValue> newImage =
                rec.getDynamodb().getNewImage();
        Assert.assertNotNull("newImage missing", newImage);

        // S and N
        Assert.assertEquals("simple string", newImage.get("attrS").getS());
        Assert.assertEquals("3.14159", newImage.get("attrN").getN());

        // B — must round-trip byte-exact, including high-bit bytes.
        byte[] roundTrippedBytes = newImage.get("attrB").getB().array();
        Assert.assertArrayEquals("Binary attribute corrupted in CDC",
                new byte[]{ 0x00, 0x01, (byte) 0xFE, (byte) 0xFF }, roundTrippedBytes);

        // BOOL
        Assert.assertTrue(newImage.get("attrBoolTrue").getBOOL());
        Assert.assertFalse(newImage.get("attrBoolFalse").getBOOL());

        // NULL
        Assert.assertTrue("NULL attribute missing or wrong type",
                newImage.get("attrNull").getNULL());

        // SS, NS, BS — order is irrelevant for sets but contents must match exactly.
        Assert.assertEquals(new java.util.HashSet<>(Arrays.asList("a", "b", "c")),
                new java.util.HashSet<>(newImage.get("attrSS").getSS()));
        Assert.assertEquals(new java.util.HashSet<>(Arrays.asList("1", "2", "3")),
                new java.util.HashSet<>(newImage.get("attrNS").getNS()));
        java.util.Set<String> bsAsStrings = new java.util.HashSet<>();
        for (java.nio.ByteBuffer bb : newImage.get("attrBS").getBS()) {
            byte[] copy = new byte[bb.remaining()];
            bb.duplicate().get(copy);
            bsAsStrings.add(new String(copy, StandardCharsets.UTF_8));
        }
        Assert.assertEquals(new java.util.HashSet<>(Arrays.asList("x", "y")), bsAsStrings);

        // L — order-preserving heterogeneous list
        List<com.amazonaws.services.dynamodbv2.model.AttributeValue> roundTrippedList =
                newImage.get("attrL").getL();
        Assert.assertEquals(3, roundTrippedList.size());
        Assert.assertEquals("a-string-in-a-list", roundTrippedList.get(0).getS());
        Assert.assertEquals("42", roundTrippedList.get(1).getN());
        Assert.assertTrue(roundTrippedList.get(2).getBOOL());

        // M — nested map
        Map<String, com.amazonaws.services.dynamodbv2.model.AttributeValue> roundTrippedMap =
                newImage.get("attrM").getM();
        Assert.assertEquals("hello", roundTrippedMap.get("nestedStr").getS());
        Assert.assertEquals("99", roundTrippedMap.get("nestedNum").getN());
        Assert.assertTrue(roundTrippedMap.get("nestedBool").getBOOL());

        // DDB parity: STRICT_TOTAL_ORDER (single record). Exercises SDK V1 AttributeValue
        // equality across every attribute type — number, byte buffer, set, nested map, list.
        KclTestUtils.awaitConsumerCaughtUp(oracle.ddbStore(), 1, Duration.ofSeconds(30));
        KclTestUtils.assertContentParity(store, oracle.ddbStore(),
                KclTestUtils.ParityMode.STRICT_TOTAL_ORDER);
    }
}
