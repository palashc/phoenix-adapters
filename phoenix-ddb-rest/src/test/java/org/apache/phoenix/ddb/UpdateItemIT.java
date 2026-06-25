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
package org.apache.phoenix.ddb;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.ReturnValuesOnConditionCheckFailure;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

import static software.amazon.awssdk.services.dynamodb.model.ReturnValue.ALL_NEW;
import static software.amazon.awssdk.services.dynamodb.model.ReturnValuesOnConditionCheckFailure.ALL_OLD;

/**
 * Tests for UpdateItem API with conditional expressions.
 * {@link UpdateItemBaseTests} for tests with different kinds of update expressions.
 */
public class UpdateItemIT extends UpdateItemBaseTests {

    public UpdateItemIT(boolean isSortKeyPresent) {
        super(isSortKeyPresent);
    }

    @Test(timeout = 120000)
    public void testConditionalCheckSuccess() {
        final String tableName = "._404-" + isSortKeyPresent + "DR1FT-Crystal_Echo__";
        createTableAndPutItem(tableName, true);

        // update item
        Map<String, AttributeValue> key = getKey();
        UpdateItemRequest.Builder uir = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir.updateExpression("SET #1 = :v1, #2 = #2 + :v2, #3 = #3 - :v3");
        uir.conditionExpression("#4.#5[0].#6 = :condVal");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "COL2");
        exprAttrNames.put("#2", "COL1");
        exprAttrNames.put("#3", "COL4");
        exprAttrNames.put("#4", "Reviews");
        exprAttrNames.put("#5", "FiveStar");
        exprAttrNames.put("#6", "reviewer");
        uir.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v1", AttributeValue.builder().s("TiTlE2").build());
        exprAttrVal.put(":v2", AttributeValue.builder().n("3.2").build());
        exprAttrVal.put(":v3", AttributeValue.builder().n("89.34").build());
        exprAttrVal.put(":condVal", AttributeValue.builder().s("Alice").build());
        uir.expressionAttributeValues(exprAttrVal);
        uir.returnValues(ALL_NEW);
        UpdateItemResponse dynamoResult = dynamoDbClient.updateItem(uir.build());
        UpdateItemResponse phoenixResult = phoenixDBClientV2.updateItem(uir.build());
        Assert.assertEquals(dynamoResult.attributes(), phoenixResult.attributes());
        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testConditionalCheckWithOldItemSuccess() {
        final String tableName = "._404-" + isSortKeyPresent + "DR12_--FT-Crystal_Echo__";
        createTableAndPutItem(tableName, true);

        Map<String, AttributeValue> key = getKey();
        UpdateItemRequest.Builder uir = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir.updateExpression("SET #1 = :v1, #2 = #2 + :v2, #3 = #3 - :v3");
        uir.conditionExpression("#4.#5[0].#6 = :condVal");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "COL2");
        exprAttrNames.put("#2", "COL1");
        exprAttrNames.put("#3", "COL4");
        exprAttrNames.put("#4", "Reviews");
        exprAttrNames.put("#5", "FiveStar");
        exprAttrNames.put("#6", "reviewer");
        uir.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v1", AttributeValue.builder().s("TiTlE2").build());
        exprAttrVal.put(":v2", AttributeValue.builder().n("3.2").build());
        exprAttrVal.put(":v3", AttributeValue.builder().n("89.34").build());
        exprAttrVal.put(":condVal", AttributeValue.builder().s("Alice").build());
        uir.expressionAttributeValues(exprAttrVal);
        uir.returnValues(ReturnValue.ALL_OLD);
        UpdateItemResponse dynamoResult = dynamoDbClient.updateItem(uir.build());
        UpdateItemResponse phoenixResult = phoenixDBClientV2.updateItem(uir.build());
        Assert.assertEquals(dynamoResult.attributes(), phoenixResult.attributes());
        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testConditionalCheckFailure() {
        final String tableName = "-_-" + isSortKeyPresent + "Ax0n.D3t0nate-Memory_Blue123....";
        createTableAndPutItem(tableName, true);
        // update item
        Map<String, AttributeValue> key = getKey();
        UpdateItemRequest.Builder uir = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir.updateExpression("REMOVE #3");
        uir.conditionExpression("#3 > :v3");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#3", "COL1");
        uir.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v3", AttributeValue.builder().n("4.3").build());
        uir.expressionAttributeValues(exprAttrVal);

        Map<String, AttributeValue> dynamoExceptionItem = null;
        try {
            dynamoDbClient.updateItem(uir.build());
            Assert.fail("UpdateItem should throw exception when condition check fails.");
        } catch (ConditionalCheckFailedException e) {
            dynamoExceptionItem = e.item();
        }
        try {
            phoenixDBClientV2.updateItem(uir.build());
            Assert.fail("UpdateItem should throw exception when condition check fails.");
        } catch (ConditionalCheckFailedException e) {
            Assert.assertEquals(dynamoExceptionItem, e.item());
        }

        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testConditionalCheckFailureReturnValue() {
        final String tableName = "__---Quasar__Glitch-Surge--O.o" + isSortKeyPresent;
        createTableAndPutItem(tableName, true);
        // update item
        Map<String, AttributeValue> key = getKey();
        UpdateItemRequest.Builder uir = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir.updateExpression("REMOVE #3");
        uir.conditionExpression("#3 > :v3");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#3", "COL1");
        uir.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v3", AttributeValue.builder().n("4.3").build());
        uir.expressionAttributeValues(exprAttrVal);
        uir.returnValuesOnConditionCheckFailure(ALL_OLD);
        Map<String, AttributeValue> dynamoReturnAttr = null, phoenixReturnAttr = null;
        try {
            dynamoDbClient.updateItem(uir.build());
            Assert.fail("UpdateItem should throw exception when condition check fails.");
        } catch (ConditionalCheckFailedException e) {
            dynamoReturnAttr = e.item();
        }
        try {
            phoenixDBClientV2.updateItem(uir.build());
            Assert.fail("UpdateItem should throw exception when condition check fails.");
        } catch (ConditionalCheckFailedException e) {
            phoenixReturnAttr = e.item();
        }
        Assert.assertEquals(dynamoReturnAttr, phoenixReturnAttr);
        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testConcurrentConditionalUpdateWithReturnValues() {
        final String tableName = "Ne0N._Crypt-x_B0tNet_Transmission-23X__" + isSortKeyPresent;
        createTableAndPutItem(tableName, true);

        ExecutorService executorService = Executors.newFixedThreadPool(5);
        AtomicInteger updateCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);

        UpdateItemRequest.Builder uir =
                UpdateItemRequest.builder().tableName(tableName).key(getKey());
        uir.updateExpression("SET #1 = #1 + :v1");
        uir.conditionExpression("#1 < :condVal");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "COL1");
        uir.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v1", AttributeValue.builder().n("10").build());
        exprAttrVal.put(":condVal", AttributeValue.builder().n("5").build());
        uir.expressionAttributeValues(exprAttrVal);
        uir.returnValues(ALL_NEW);
        uir.returnValuesOnConditionCheckFailure(ALL_OLD);
        Map<String, AttributeValue> newItem = dynamoDbClient.updateItem(uir.build()).attributes();

        for (int i = 0; i < 5; i++) {
            executorService.submit(() -> {
                Map<String, AttributeValue> oldItem = null;
                try {
                    dynamoDbClient.updateItem(uir.build());
                } catch (ConditionalCheckFailedException e) {
                    oldItem = e.item();
                }
                try {
                    UpdateItemResponse result = phoenixDBClientV2.updateItem(uir.build());
                    Assert.assertEquals(newItem, result.attributes());
                    updateCount.incrementAndGet();
                } catch (ConditionalCheckFailedException e) {
                    Assert.assertEquals(oldItem, e.item());
                    errorCount.incrementAndGet();
                }
            });
        }
        executorService.shutdown();
        try {
            boolean terminated = executorService.awaitTermination(30, TimeUnit.SECONDS);
            if (terminated) {
                Assert.assertEquals(1, updateCount.get());
                Assert.assertEquals(4, errorCount.get());
            } else {
                Assert.fail(
                        "testConcurrentConditionalUpdateWithReturnValues: threads did not terminate.");
            }
        } catch (InterruptedException e) {
            Assert.fail("testConcurrentConditionalUpdateWithReturnValues was interrupted.");
        }
    }

    /**
     * Test ADD operation creating a new item.
     * DynamoDB semantics: ADD on non-existing item should create item with ADD value.
     */
    @Test(timeout = 120000)
    public void testAddOperationCreateNewItem() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, false);

        Map<String, AttributeValue> key = getKey();

        // ADD operation on non-existing item
        UpdateItemRequest updateRequest = UpdateItemRequest.builder().tableName(tableName).key(key)
                .updateExpression("ADD numericField :val").expressionAttributeValues(
                        Collections.singletonMap(":val", AttributeValue.builder().n("5").build()))
                .returnValues(ALL_NEW).build();

        // Execute on both DynamoDB and Phoenix
        UpdateItemResponse dynamoResponse = dynamoDbClient.updateItem(updateRequest);
        UpdateItemResponse phoenixResponse = phoenixDBClientV2.updateItem(updateRequest);

        // Verify both responses match
        Assert.assertEquals("ADD operation responses should match", dynamoResponse.attributes(),
                phoenixResponse.attributes());

        // Verify the item was created with correct value
        Assert.assertNotNull("Item should be created", dynamoResponse.attributes());
        Assert.assertEquals("Numeric field should have ADD value", "5",
                dynamoResponse.attributes().get("numericField").n());
        
        // Verify final state by querying both Phoenix and DDB
        validateItem(tableName, key);
    }

    /**
     * Test mixed SET and ADD operations creating a new item.
     */
    @Test(timeout = 120000)
    public void testMixedSetAddOperationCreateNewItem() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, false);

        Map<String, AttributeValue> key = getKey();

        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":str", AttributeValue.builder().s("test").build());
        expressionAttributeValues.put(":num", AttributeValue.builder().n("10").build());

        UpdateItemRequest updateRequest = UpdateItemRequest.builder().tableName(tableName).key(key)
                .updateExpression("SET stringField = :str ADD numericField :num")
                .expressionAttributeValues(expressionAttributeValues).returnValues(ALL_NEW).build();

        UpdateItemResponse dynamoResponse = dynamoDbClient.updateItem(updateRequest);
        UpdateItemResponse phoenixResponse = phoenixDBClientV2.updateItem(updateRequest);

        Assert.assertEquals("Mixed operation responses should match", dynamoResponse.attributes(),
                phoenixResponse.attributes());
        Assert.assertEquals("String field should be set", "test",
                dynamoResponse.attributes().get("stringField").s());
        Assert.assertEquals("Numeric field should have ADD value", "10",
                dynamoResponse.attributes().get("numericField").n());
        
        // Verify final state by querying both Phoenix and DDB
        validateItem(tableName, key);
    }

    /**
     * Test REMOVE-only operation on non-existing item (should be no-op).
     */
    @Test(timeout = 120000)
    public void testRemoveOnlyOperationOnNonExistingItem() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, false);

        Map<String, AttributeValue> key = getKey();

        UpdateItemRequest updateRequest = UpdateItemRequest.builder().tableName(tableName).key(key)
                .updateExpression("REMOVE nonExistentField").returnValues(ALL_NEW).build();

        UpdateItemResponse dynamoResponse = dynamoDbClient.updateItem(updateRequest);
        UpdateItemResponse phoenixResponse = phoenixDBClientV2.updateItem(updateRequest);

        // REMOVE on non-existing item should create empty item (keys only)
        Assert.assertEquals("REMOVE operation responses should match", dynamoResponse.attributes(),
                phoenixResponse.attributes());

        // Verify final state by querying both Phoenix and DDB
        validateItem(tableName, key);
    }

    /**
     * Test condition that can be satisfied on empty document.
     */
    @Test(timeout = 120000)
    public void testConditionTrueOnEmptyDocument() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, false);

        Map<String, AttributeValue> key = getKey();

        // Condition "attribute_not_exists(someField)" should be true on empty document
        UpdateItemRequest updateRequest = UpdateItemRequest.builder().tableName(tableName).key(key)
                .updateExpression("SET newField = :val")
                .conditionExpression("attribute_not_exists(someField)").expressionAttributeValues(
                        Collections.singletonMap(":val",
                                AttributeValue.builder().s("created").build()))
                .returnValues(ALL_NEW).build();

        UpdateItemResponse dynamoResponse = dynamoDbClient.updateItem(updateRequest);
        UpdateItemResponse phoenixResponse = phoenixDBClientV2.updateItem(updateRequest);

        Assert.assertEquals("Conditional create responses should match",
                dynamoResponse.attributes(), phoenixResponse.attributes());

        // Verify final state by querying both Phoenix and DDB
        validateItem(tableName, key);
    }

    /**
     * Test condition that cannot be satisfied on empty document.
     */
    @Test(timeout = 120000)
    public void testConditionFalseOnEmptyDocument() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, false);

        Map<String, AttributeValue> key = getKey();

        // Condition "attribute_exists(someField)" should be false on empty document
        UpdateItemRequest updateRequest = UpdateItemRequest.builder().tableName(tableName).key(key)
                .updateExpression("SET newField = :val")
                .returnValuesOnConditionCheckFailure(ALL_OLD)
                .conditionExpression("attribute_exists(someField)").expressionAttributeValues(
                        Collections.singletonMap(":val",
                                AttributeValue.builder().s("shouldNotCreate").build())).build();

        // Both should throw ConditionalCheckFailedException
        ConditionalCheckFailedException ddbException = null;
        try {
            dynamoDbClient.updateItem(updateRequest);
            Assert.fail("DynamoDB should throw ConditionalCheckFailedException");
        } catch (ConditionalCheckFailedException e) {
            ddbException = e;
        }

        try {
            phoenixDBClientV2.updateItem(updateRequest);
            Assert.fail("Phoenix should throw ConditionalCheckFailedException");
        } catch (ConditionalCheckFailedException e) {
            Assert.assertEquals(ddbException.hasItem(), e.hasItem());
            Assert.assertEquals(ddbException.item(), e.item());
        }

        // Verify final state by querying both Phoenix and DDB
        validateItem(tableName, key);
    }

    /**
     * Test ADD operation on existing item (should add to existing value).
     */
    @Test(timeout = 120000)
    public void testAddOperationOnExistingItem() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, false);

        Map<String, AttributeValue> key = getKey();

        // First, set a numeric field
        UpdateItemRequest setRequest = UpdateItemRequest.builder().tableName(tableName).key(key)
                .updateExpression("SET numericField = :val").expressionAttributeValues(
                        Collections.singletonMap(":val", AttributeValue.builder().n("5").build()))
                .build();

        dynamoDbClient.updateItem(setRequest);
        phoenixDBClientV2.updateItem(setRequest);

        // Now ADD to the existing numeric field
        UpdateItemRequest addRequest = UpdateItemRequest.builder().tableName(tableName).key(key)
                .updateExpression("ADD numericField :val").expressionAttributeValues(
                        Collections.singletonMap(":val", AttributeValue.builder().n("3").build()))
                .returnValues(ALL_NEW).build();

        UpdateItemResponse dynamoResponse = dynamoDbClient.updateItem(addRequest);
        UpdateItemResponse phoenixResponse = phoenixDBClientV2.updateItem(addRequest);

        Assert.assertEquals("ADD to existing responses should match", dynamoResponse.attributes(),
                phoenixResponse.attributes());
        
        // Verify final state by querying both Phoenix and DDB
        validateItem(tableName, key);
    }

    /**
     * Test ADD operation with StringSet on non-existing item.
     * DynamoDB semantics: ADD on non-existing item should create item with ADD value.
     */
    @Test(timeout = 120000)
    public void testAddStringSetOnNonExistingItem() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, false);

        Map<String, AttributeValue> key = getKey();

        // ADD StringSet operation on non-existing item
        UpdateItemRequest updateRequest = UpdateItemRequest.builder()
                .tableName(tableName)
                .key(key)
                .updateExpression("ADD stringSet :val")
                .expressionAttributeValues(Collections.singletonMap(":val", 
                        AttributeValue.builder().ss("value1", "value2").build()))
                .returnValues(ALL_NEW)
                .build();

        // Execute on both DynamoDB and Phoenix
        UpdateItemResponse dynamoResponse = dynamoDbClient.updateItem(updateRequest);
        UpdateItemResponse phoenixResponse = phoenixDBClientV2.updateItem(updateRequest);

        // Verify both responses match
        Assert.assertEquals("ADD StringSet operation responses should match", 
                dynamoResponse.attributes(), phoenixResponse.attributes());

        // Verify final state by querying both Phoenix and DDB
        validateItem(tableName, key);
    }

    /**
     * Test ADD operation with NumberSet on non-existing item.
     * DynamoDB semantics: ADD on non-existing item should create item with ADD value.
     */
    @Test(timeout = 120000)
    public void testAddNumberSetOnNonExistingItem() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, false);

        Map<String, AttributeValue> key = getKey();

        // ADD NumberSet operation on non-existing item
        UpdateItemRequest updateRequest = UpdateItemRequest.builder()
                .tableName(tableName)
                .key(key)
                .updateExpression("ADD numberSet :val")
                .expressionAttributeValues(Collections.singletonMap(":val", 
                        AttributeValue.builder().ns("1", "2", "3").build()))
                .returnValues(ALL_NEW)
                .build();

        // Execute on both DynamoDB and Phoenix
        UpdateItemResponse dynamoResponse = dynamoDbClient.updateItem(updateRequest);
        UpdateItemResponse phoenixResponse = phoenixDBClientV2.updateItem(updateRequest);

        // Verify both responses match
        Assert.assertEquals("ADD NumberSet operation responses should match", 
                dynamoResponse.attributes(), phoenixResponse.attributes());

        // Verify final state by querying both Phoenix and DDB
        validateItem(tableName, key);
    }

    /**
     * Test ADD operation with BinarySet on non-existing item.
     * DynamoDB semantics: ADD on non-existing item should create item with ADD value.
     */
    @Test(timeout = 120000)
    public void testUpdateItemReturnValuesValidation() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, false);

        Map<String, AttributeValue> key = getKey();
        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":val", AttributeValue.builder().s("test").build());

        // Test NONE - should succeed
        UpdateItemRequest updateRequest = UpdateItemRequest.builder().tableName(tableName).key(key)
            .updateExpression("SET stringField = :val")
            .expressionAttributeValues(expressionAttributeValues).returnValues(ReturnValue.NONE)
            .build();
        UpdateItemResponse dynamoResult1 = dynamoDbClient.updateItem(updateRequest);
        UpdateItemResponse phoenixResult1 = phoenixDBClientV2.updateItem(updateRequest);
        Assert.assertEquals(dynamoResult1.attributes(), phoenixResult1.attributes());

        // Test ALL_OLD - should succeed
        updateRequest = UpdateItemRequest.builder().tableName(tableName).key(key)
            .updateExpression("SET stringField = :val")
            .expressionAttributeValues(expressionAttributeValues).returnValues(ReturnValue.ALL_OLD)
            .build();
        UpdateItemResponse dynamoResult2 = dynamoDbClient.updateItem(updateRequest);
        UpdateItemResponse phoenixResult2 = phoenixDBClientV2.updateItem(updateRequest);
        Assert.assertEquals(dynamoResult2.attributes(), phoenixResult2.attributes());

        // Test ALL_NEW - should succeed
        updateRequest = UpdateItemRequest.builder().tableName(tableName).key(key)
            .updateExpression("SET stringField = :val")
            .expressionAttributeValues(expressionAttributeValues).returnValues(ALL_NEW)
            .build();
        UpdateItemResponse dynamoResult3 = dynamoDbClient.updateItem(updateRequest);
        UpdateItemResponse phoenixResult3 = phoenixDBClientV2.updateItem(updateRequest);
        Assert.assertEquals(dynamoResult3.attributes(), phoenixResult3.attributes());

        // Test UPDATED_OLD - now supported on UpdateItem; returns only the touched attribute
        // paths (here: just `stringField`) with their pre-update values.
        updateRequest = UpdateItemRequest.builder().tableName(tableName).key(key)
            .updateExpression("SET stringField = :val")
            .expressionAttributeValues(expressionAttributeValues)
            .returnValues(ReturnValue.UPDATED_OLD).build();
        UpdateItemResponse dynamoResult4 = dynamoDbClient.updateItem(updateRequest);
        UpdateItemResponse phoenixResult4 = phoenixDBClientV2.updateItem(updateRequest);
        Assert.assertEquals(dynamoResult4.attributes(), phoenixResult4.attributes());

        // Test UPDATED_NEW - now supported on UpdateItem; returns only the touched attribute
        // paths with their post-update values.
        updateRequest = UpdateItemRequest.builder().tableName(tableName).key(key)
            .updateExpression("SET stringField = :val")
            .expressionAttributeValues(expressionAttributeValues)
            .returnValues(ReturnValue.UPDATED_NEW).build();
        UpdateItemResponse dynamoResult5 = dynamoDbClient.updateItem(updateRequest);
        UpdateItemResponse phoenixResult5 = phoenixDBClientV2.updateItem(updateRequest);
        Assert.assertEquals(dynamoResult5.attributes(), phoenixResult5.attributes());

        // Test invalid value - should fail with same error status code in both clients
        updateRequest = UpdateItemRequest.builder().tableName(tableName).key(key)
            .updateExpression("SET stringField = :val")
            .expressionAttributeValues(expressionAttributeValues).returnValues("INVALID_VALUE")
            .build();

        int phoenixStatusCode = -1;
        int dynamoStatusCode = -1;
        try {
            dynamoDbClient.updateItem(updateRequest);
            Assert.fail("Expected DynamoDbException for invalid value");
        } catch (DynamoDbException e) {
            dynamoStatusCode = e.statusCode();
        }
        try {
            phoenixDBClientV2.updateItem(updateRequest);
            Assert.fail("Expected DynamoDbException for invalid value");
        } catch (DynamoDbException e) {
            phoenixStatusCode = e.statusCode();
            Assert.assertTrue(e.getMessage()
                .contains("ReturnValues value 'INVALID_VALUE' is not valid for UpdateItem"));
        }
        Assert.assertEquals("Status codes should match for INVALID_VALUE validation error",
            dynamoStatusCode, phoenixStatusCode);
    }

    @Test(timeout = 120000)
    public void testUpdateItemReturnValuesOnConditionCheckFailureValidation() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, false);

        Map<String, AttributeValue> key = getKey();
        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":val", AttributeValue.builder().s("test").build());

        // Test NONE - should succeed (validation passes)
        UpdateItemRequest updateRequest = UpdateItemRequest.builder().tableName(tableName).key(key)
            .updateExpression("SET stringField = :val")
            .expressionAttributeValues(expressionAttributeValues)
            .returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.NONE).build();
        UpdateItemResponse dynamoResult1 = dynamoDbClient.updateItem(updateRequest);
        UpdateItemResponse phoenixResult1 = phoenixDBClientV2.updateItem(updateRequest);
        Assert.assertEquals(dynamoResult1.attributes(), phoenixResult1.attributes());

        // Test ALL_OLD - should succeed (validation passes)
        updateRequest = UpdateItemRequest.builder().tableName(tableName).key(key)
            .updateExpression("SET stringField = :val")
            .expressionAttributeValues(expressionAttributeValues)
            .returnValuesOnConditionCheckFailure(ALL_OLD)
            .build();
        UpdateItemResponse dynamoResult2 = dynamoDbClient.updateItem(updateRequest);
        UpdateItemResponse phoenixResult2 = phoenixDBClientV2.updateItem(updateRequest);
        Assert.assertEquals(dynamoResult2.attributes(), phoenixResult2.attributes());

        // Test invalid value - should fail with same error status code in both clients
        updateRequest = UpdateItemRequest.builder().tableName(tableName).key(key)
            .updateExpression("SET stringField = :val")
            .expressionAttributeValues(expressionAttributeValues)
            .returnValuesOnConditionCheckFailure("INVALID_VALUE").build();
        int dynamoStatusCode = -1;
        int phoenixStatusCode = -1;
        try {
            dynamoDbClient.updateItem(updateRequest);
            Assert.fail("Expected DynamoDbException for invalid value");
        } catch (DynamoDbException e) {
            dynamoStatusCode = e.statusCode();
        }
        try {
            phoenixDBClientV2.updateItem(updateRequest);
            Assert.fail("Expected DynamoDbException for invalid value");
        } catch (DynamoDbException e) {
            phoenixStatusCode = e.statusCode();
            Assert.assertTrue(e.getMessage().contains(
                "ReturnValuesOnConditionCheckFailure value 'INVALID_VALUE' is not valid"));
        }
        Assert.assertEquals("Status codes should match for INVALID_VALUE validation error",
            dynamoStatusCode, phoenixStatusCode);
    }

    @Test(timeout = 120000)
    public void testAddBinarySetOnNonExistingItem() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, false);

        Map<String, AttributeValue> key = getKey();

        // ADD BinarySet operation on non-existing item
        SdkBytes binary1 = SdkBytes.fromUtf8String("data1");
        SdkBytes binary2 = SdkBytes.fromUtf8String("data2");
        
        UpdateItemRequest updateRequest = UpdateItemRequest.builder()
                .tableName(tableName)
                .key(key)
                .updateExpression("ADD binarySet :val")
                .expressionAttributeValues(Collections.singletonMap(":val", 
                        AttributeValue.builder().bs(binary1, binary2).build()))
                .returnValues(ALL_NEW)
                .build();

        // Execute on both DynamoDB and Phoenix
        UpdateItemResponse dynamoResponse = dynamoDbClient.updateItem(updateRequest);
        UpdateItemResponse phoenixResponse = phoenixDBClientV2.updateItem(updateRequest);

        // Verify both responses match
        Assert.assertEquals("ADD BinarySet operation responses should match", 
                dynamoResponse.attributes(), phoenixResponse.attributes());

        // Verify final state by querying both Phoenix and DDB
        validateItem(tableName, key);
    }

    // ---------------------------------------------------------------------------------
    // Canonical DynamoDB UpdateExpression docs examples. Each test seeds a custom item
    // and asserts the adapter and real DDB end up with byte-equal items.
    // ---------------------------------------------------------------------------------

    /**
     * Docs example: SET that simultaneously updates a list element and a scalar.
     * "SET RelatedItems[1] = :newValue, Price = :newPrice"
     */
    @Test(timeout = 120000)
    public void testDocsSetListElementAndScalarTogether() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, false);
        Map<String, AttributeValue> key = getKey();

        Map<String, AttributeValue> item = new HashMap<>(key);
        item.put("Price", AttributeValue.builder().n("52").build());
        item.put("RelatedItems", AttributeValue.builder().l(
                AttributeValue.builder().s("Hammer").build(),
                AttributeValue.builder().s("Nails").build()).build());
        seedItem(tableName, item);

        Map<String, AttributeValue> values = new HashMap<>();
        values.put(":newValue", AttributeValue.builder().s("Screwdriver").build());
        values.put(":newPrice", AttributeValue.builder().n("60").build());
        runUpdateAndCompare(tableName, key,
                "SET RelatedItems[1] = :newValue, Price = :newPrice", values);
    }

    /**
     * Docs example: SET arithmetic combined with REMOVE in the same expression.
     * "SET Price = Price - :p REMOVE InStock"
     */
    @Test(timeout = 120000)
    public void testDocsSetArithmeticAndRemoveTogether() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, false);
        Map<String, AttributeValue> key = getKey();

        Map<String, AttributeValue> item = new HashMap<>(key);
        item.put("Price", AttributeValue.builder().n("60").build());
        item.put("InStock", AttributeValue.builder().bool(true).build());
        seedItem(tableName, item);

        Map<String, AttributeValue> values = Collections.singletonMap(":p",
                AttributeValue.builder().n("15").build());
        runUpdateAndCompare(tableName, key,
                "SET Price = Price - :p REMOVE InStock", values);
    }

    /**
     * Docs example: REMOVE multiple list elements in one expression. The remaining
     * elements compact (shift), matching the documented behaviour.
     */
    @Test(timeout = 120000)
    public void testDocsRemoveMultipleListElements() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, false);
        Map<String, AttributeValue> key = getKey();

        Map<String, AttributeValue> item = new HashMap<>(key);
        item.put("RelatedItems", AttributeValue.builder().l(
                AttributeValue.builder().s("Chisel").build(),
                AttributeValue.builder().s("Hammer").build(),
                AttributeValue.builder().s("Nails").build(),
                AttributeValue.builder().s("Screwdriver").build(),
                AttributeValue.builder().s("Hacksaw").build()).build());
        seedItem(tableName, item);
        runUpdateAndCompare(tableName, key,
                "REMOVE RelatedItems[1], RelatedItems[2]", null);
    }

    /**
     * Remove first (head) element of a list.
     */
    @Test(timeout = 120000)
    public void testDocsRemoveListHead() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, false);
        Map<String, AttributeValue> key = getKey();

        Map<String, AttributeValue> item = new HashMap<>(key);
        item.put("L", AttributeValue.builder().l(
                AttributeValue.builder().s("a").build(),
                AttributeValue.builder().s("b").build(),
                AttributeValue.builder().s("c").build()).build());
        seedItem(tableName, item);
        runUpdateAndCompare(tableName, key, "REMOVE L[0]", null);
    }

    /**
     * Remove last (tail) element of a list.
     */
    @Test(timeout = 120000)
    public void testDocsRemoveListTail() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, false);
        Map<String, AttributeValue> key = getKey();

        Map<String, AttributeValue> item = new HashMap<>(key);
        item.put("L", AttributeValue.builder().l(
                AttributeValue.builder().s("a").build(),
                AttributeValue.builder().s("b").build(),
                AttributeValue.builder().s("c").build()).build());
        seedItem(tableName, item);
        runUpdateAndCompare(tableName, key, "REMOVE L[2]", null);
    }

    /**
     * if_not_exists where the path DOES exist returns the existing value, not the fallback.
     */
    @Test(timeout = 120000)
    public void testDocsIfNotExistsBranchExisting() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, false);
        Map<String, AttributeValue> key = getKey();

        Map<String, AttributeValue> item = new HashMap<>(key);
        item.put("Title", AttributeValue.builder().s("OriginalTitle").build());
        seedItem(tableName, item);

        Map<String, AttributeValue> values = Collections.singletonMap(":t",
                AttributeValue.builder().s("FallbackTitle").build());
        runUpdateAndCompare(tableName, key,
                "SET Title = if_not_exists(Title, :t)", values);
    }

    /**
     * if_not_exists where the path is missing returns the fallback value.
     */
    @Test(timeout = 120000)
    public void testDocsIfNotExistsBranchMissing() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, false);
        Map<String, AttributeValue> key = getKey();

        seedItem(tableName, new HashMap<>(key));

        Map<String, AttributeValue> values = Collections.singletonMap(":t",
                AttributeValue.builder().s("FallbackTitle").build());
        runUpdateAndCompare(tableName, key,
                "SET Title = if_not_exists(Title, :t)", values);
    }

    /**
     * DELETE elements from a Number Set (NS).
     */
    @Test(timeout = 120000)
    public void testDocsDeleteFromNumberSet() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, false);
        Map<String, AttributeValue> key = getKey();

        Map<String, AttributeValue> item = new HashMap<>(key);
        item.put("Scores", AttributeValue.builder().ns("1", "2", "3", "4").build());
        seedItem(tableName, item);

        Map<String, AttributeValue> values = Collections.singletonMap(":r",
                AttributeValue.builder().ns("2", "4").build());
        runUpdateAndCompare(tableName, key, "DELETE Scores :r", values);
    }

    /**
     * DELETE elements from a Binary Set (BS).
     */
    @Test(timeout = 120000)
    public void testDocsDeleteFromBinarySet() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, false);
        Map<String, AttributeValue> key = getKey();

        Map<String, AttributeValue> item = new HashMap<>(key);
        item.put("BinSet", AttributeValue.builder().bs(
                SdkBytes.fromByteArray(new byte[] {1}),
                SdkBytes.fromByteArray(new byte[] {2}),
                SdkBytes.fromByteArray(new byte[] {3})).build());
        seedItem(tableName, item);

        Map<String, AttributeValue> values = Collections.singletonMap(":r",
                AttributeValue.builder().bs(SdkBytes.fromByteArray(new byte[] {2})).build());
        runUpdateAndCompare(tableName, key, "DELETE BinSet :r", values);
    }

    /**
     * ADD into a Binary Set (BS) on an existing set.
     */
    @Test(timeout = 120000)
    public void testDocsAddBinarySet() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, false);
        Map<String, AttributeValue> key = getKey();

        Map<String, AttributeValue> item = new HashMap<>(key);
        item.put("BinSet", AttributeValue.builder().bs(
                SdkBytes.fromByteArray(new byte[] {1})).build());
        seedItem(tableName, item);

        Map<String, AttributeValue> values = Collections.singletonMap(":a",
                AttributeValue.builder().bs(
                        SdkBytes.fromByteArray(new byte[] {2}),
                        SdkBytes.fromByteArray(new byte[] {3})).build());
        runUpdateAndCompare(tableName, key, "ADD BinSet :a", values);
    }

    /**
     * list_append on a list whose elements are themselves Maps.
     */
    @Test(timeout = 120000)
    public void testDocsListAppendOnListOfMaps() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, false);
        Map<String, AttributeValue> key = getKey();

        Map<String, AttributeValue> existing = new HashMap<>();
        existing.put("name", AttributeValue.builder().s("alice").build());
        Map<String, AttributeValue> item = new HashMap<>(key);
        item.put("Audit", AttributeValue.builder().l(
                AttributeValue.builder().m(existing).build()).build());
        seedItem(tableName, item);

        Map<String, AttributeValue> appended = new HashMap<>();
        appended.put("name", AttributeValue.builder().s("bob").build());
        Map<String, AttributeValue> values = Collections.singletonMap(":new",
                AttributeValue.builder().l(
                        AttributeValue.builder().m(appended).build()).build());
        runUpdateAndCompare(tableName, key,
                "SET Audit = list_append(Audit, :new)", values);
    }

    private void seedItem(String tableName, Map<String, AttributeValue> item) {
        PutItemRequest put = PutItemRequest.builder().tableName(tableName).item(item).build();
        dynamoDbClient.putItem(put);
        phoenixDBClientV2.putItem(put);
    }

    private void runUpdateAndCompare(String tableName, Map<String, AttributeValue> key,
            String updateExpression, Map<String, AttributeValue> values) {
        UpdateItemRequest.Builder b = UpdateItemRequest.builder().tableName(tableName).key(key)
                .updateExpression(updateExpression);
        if (values != null) {
            b.expressionAttributeValues(values);
        }
        UpdateItemRequest req = b.build();
        dynamoDbClient.updateItem(req);
        phoenixDBClientV2.updateItem(req);
        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testUpdatedOldTopLevelSet() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);
        Map<String, AttributeValue> key = getKey();

        UpdateItemRequest.Builder uir = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir.updateExpression("SET COL2 = :v1, COL1 = :v2");
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v1", AttributeValue.builder().s("NewTitle").build());
        exprAttrVal.put(":v2", AttributeValue.builder().n("999").build());
        uir.expressionAttributeValues(exprAttrVal);
        uir.returnValues(ReturnValue.UPDATED_OLD);
        UpdateItemResponse dynamoResult = dynamoDbClient.updateItem(uir.build());
        UpdateItemResponse phoenixResult = phoenixDBClientV2.updateItem(uir.build());
        Assert.assertEquals(dynamoResult.attributes(), phoenixResult.attributes());
        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testUpdatedNewTopLevelSet() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);
        Map<String, AttributeValue> key = getKey();

        UpdateItemRequest.Builder uir = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir.updateExpression("SET COL2 = :v1, COL1 = :v2");
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v1", AttributeValue.builder().s("NewTitle").build());
        exprAttrVal.put(":v2", AttributeValue.builder().n("999").build());
        uir.expressionAttributeValues(exprAttrVal);
        uir.returnValues(ReturnValue.UPDATED_NEW);
        UpdateItemResponse dynamoResult = dynamoDbClient.updateItem(uir.build());
        UpdateItemResponse phoenixResult = phoenixDBClientV2.updateItem(uir.build());
        Assert.assertEquals(dynamoResult.attributes(), phoenixResult.attributes());
        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testUpdatedOldNewWithRemove() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);
        Map<String, AttributeValue> key = getKey();

        UpdateItemRequest.Builder uir1 = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir1.updateExpression("REMOVE COL3");
        uir1.returnValues(ReturnValue.UPDATED_OLD);
        UpdateItemResponse dynamoResult1 = dynamoDbClient.updateItem(uir1.build());
        UpdateItemResponse phoenixResult1 = phoenixDBClientV2.updateItem(uir1.build());
        Assert.assertEquals(dynamoResult1.attributes(), phoenixResult1.attributes());

        UpdateItemRequest.Builder uir2 = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir2.updateExpression("REMOVE COL2");
        uir2.returnValues(ReturnValue.UPDATED_NEW);
        UpdateItemResponse dynamoResult2 = dynamoDbClient.updateItem(uir2.build());
        UpdateItemResponse phoenixResult2 = phoenixDBClientV2.updateItem(uir2.build());
        Assert.assertEquals(dynamoResult2.attributes(), phoenixResult2.attributes());

        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testUpdatedOldNewWithAddNumber() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);
        Map<String, AttributeValue> key = getKey();

        UpdateItemRequest.Builder uir1 = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir1.updateExpression("ADD #c :v");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#c", "Counter");
        uir1.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal1 = new HashMap<>();
        exprAttrVal1.put(":v", AttributeValue.builder().n("5").build());
        uir1.expressionAttributeValues(exprAttrVal1);
        uir1.returnValues(ReturnValue.UPDATED_OLD);
        UpdateItemResponse dynamoResult1 = dynamoDbClient.updateItem(uir1.build());
        UpdateItemResponse phoenixResult1 = phoenixDBClientV2.updateItem(uir1.build());
        Assert.assertEquals(dynamoResult1.attributes(), phoenixResult1.attributes());

        // Re-seed so the second half starts from the same baseline (Counter == 66).
        seedItem(tableName, getItem1());

        UpdateItemRequest.Builder uir2 = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir2.updateExpression("ADD #c :v");
        uir2.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal2 = new HashMap<>();
        exprAttrVal2.put(":v", AttributeValue.builder().n("4").build());
        uir2.expressionAttributeValues(exprAttrVal2);
        uir2.returnValues(ReturnValue.UPDATED_NEW);
        UpdateItemResponse dynamoResult2 = dynamoDbClient.updateItem(uir2.build());
        UpdateItemResponse phoenixResult2 = phoenixDBClientV2.updateItem(uir2.build());
        Assert.assertEquals(dynamoResult2.attributes(), phoenixResult2.attributes());

        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testUpdatedOldNewWithAddOnNonExistingAttr() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);
        Map<String, AttributeValue> key = getKey();

        UpdateItemRequest.Builder uir1 = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir1.updateExpression("ADD FreshCounter :v");
        Map<String, AttributeValue> exprAttrVal1 = new HashMap<>();
        exprAttrVal1.put(":v", AttributeValue.builder().n("7").build());
        uir1.expressionAttributeValues(exprAttrVal1);
        uir1.returnValues(ReturnValue.UPDATED_OLD);
        UpdateItemResponse dynamoResult1 = dynamoDbClient.updateItem(uir1.build());
        UpdateItemResponse phoenixResult1 = phoenixDBClientV2.updateItem(uir1.build());
        Assert.assertEquals(dynamoResult1.attributes(), phoenixResult1.attributes());

        UpdateItemRequest.Builder uir2 = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir2.updateExpression("ADD AnotherFresh :v");
        Map<String, AttributeValue> exprAttrVal2 = new HashMap<>();
        exprAttrVal2.put(":v", AttributeValue.builder().n("3").build());
        uir2.expressionAttributeValues(exprAttrVal2);
        uir2.returnValues(ReturnValue.UPDATED_NEW);
        UpdateItemResponse dynamoResult2 = dynamoDbClient.updateItem(uir2.build());
        UpdateItemResponse phoenixResult2 = phoenixDBClientV2.updateItem(uir2.build());
        Assert.assertEquals(dynamoResult2.attributes(), phoenixResult2.attributes());

        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testUpdatedOldNewNestedPath() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);
        Map<String, AttributeValue> key = getKey();

        // Reviews.FiveStar[0].reviewer starts at "Alice" (see getItem1()).
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#r", "Reviews");
        exprAttrNames.put("#f", "FiveStar");
        exprAttrNames.put("#rv", "reviewer");

        UpdateItemRequest.Builder uir1 = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir1.updateExpression("SET #r.#f[0].#rv = :v");
        uir1.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal1 = new HashMap<>();
        exprAttrVal1.put(":v", AttributeValue.builder().s("Carol").build());
        uir1.expressionAttributeValues(exprAttrVal1);
        uir1.returnValues(ReturnValue.UPDATED_OLD);
        UpdateItemResponse dynamoResult1 = dynamoDbClient.updateItem(uir1.build());
        UpdateItemResponse phoenixResult1 = phoenixDBClientV2.updateItem(uir1.build());
        Assert.assertEquals(dynamoResult1.attributes(), phoenixResult1.attributes());

        UpdateItemRequest.Builder uir2 = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir2.updateExpression("SET #r.#f[0].#rv = :v");
        uir2.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal2 = new HashMap<>();
        exprAttrVal2.put(":v", AttributeValue.builder().s("Dave").build());
        uir2.expressionAttributeValues(exprAttrVal2);
        uir2.returnValues(ReturnValue.UPDATED_NEW);
        UpdateItemResponse dynamoResult2 = dynamoDbClient.updateItem(uir2.build());
        UpdateItemResponse phoenixResult2 = phoenixDBClientV2.updateItem(uir2.build());
        Assert.assertEquals(dynamoResult2.attributes(), phoenixResult2.attributes());

        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testUpdatedOldNewMixedClauses() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);
        Map<String, AttributeValue> key = getKey();

        UpdateItemRequest.Builder uir1 = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir1.updateExpression("SET COL2 = :s REMOVE COL3 ADD COL1 :n");
        Map<String, AttributeValue> exprAttrVal1 = new HashMap<>();
        exprAttrVal1.put(":s", AttributeValue.builder().s("UPD").build());
        exprAttrVal1.put(":n", AttributeValue.builder().n("100").build());
        uir1.expressionAttributeValues(exprAttrVal1);
        uir1.returnValues(ReturnValue.UPDATED_OLD);
        UpdateItemResponse dynamoResult1 = dynamoDbClient.updateItem(uir1.build());
        UpdateItemResponse phoenixResult1 = phoenixDBClientV2.updateItem(uir1.build());
        Assert.assertEquals(dynamoResult1.attributes(), phoenixResult1.attributes());

        // Reseed so the NEW half runs against the same baseline as the OLD half.
        seedItem(tableName, getItem1());

        UpdateItemRequest.Builder uir2 = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir2.updateExpression("SET COL2 = :s REMOVE COL3 ADD COL1 :n");
        Map<String, AttributeValue> exprAttrVal2 = new HashMap<>();
        exprAttrVal2.put(":s", AttributeValue.builder().s("UPD2").build());
        exprAttrVal2.put(":n", AttributeValue.builder().n("50").build());
        uir2.expressionAttributeValues(exprAttrVal2);
        uir2.returnValues(ReturnValue.UPDATED_NEW);
        UpdateItemResponse dynamoResult2 = dynamoDbClient.updateItem(uir2.build());
        UpdateItemResponse phoenixResult2 = phoenixDBClientV2.updateItem(uir2.build());
        Assert.assertEquals(dynamoResult2.attributes(), phoenixResult2.attributes());

        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testUpdatedNewOnItemCreation() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        // Note: createTableAndPutItem(_, false) creates the table but does NOT seed any row.
        createTableAndPutItem(tableName, false);
        Map<String, AttributeValue> key = getKey();

        UpdateItemRequest.Builder uir = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir.updateExpression("SET stringField = :s ADD #c :n");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#c", "counter");
        uir.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":s", AttributeValue.builder().s("hello").build());
        exprAttrVal.put(":n", AttributeValue.builder().n("9").build());
        uir.expressionAttributeValues(exprAttrVal);
        uir.returnValues(ReturnValue.UPDATED_NEW);
        UpdateItemResponse dynamoResult = dynamoDbClient.updateItem(uir.build());
        UpdateItemResponse phoenixResult = phoenixDBClientV2.updateItem(uir.build());
        Assert.assertEquals(dynamoResult.attributes(), phoenixResult.attributes());
        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testUpdatedOldOnItemCreation() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, false);
        Map<String, AttributeValue> key = getKey();

        UpdateItemRequest.Builder uir = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir.updateExpression("SET stringField = :s");
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":s", AttributeValue.builder().s("hello").build());
        uir.expressionAttributeValues(exprAttrVal);
        uir.returnValues(ReturnValue.UPDATED_OLD);
        UpdateItemResponse dynamoResult = dynamoDbClient.updateItem(uir.build());
        UpdateItemResponse phoenixResult = phoenixDBClientV2.updateItem(uir.build());
        Assert.assertEquals(dynamoResult.attributes(), phoenixResult.attributes());
        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testUpdatedNewConditionalSuccess() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);
        Map<String, AttributeValue> key = getKey();

        UpdateItemRequest.Builder uir = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir.updateExpression("SET COL2 = :s");
        uir.conditionExpression("COL1 = :cond");
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":s", AttributeValue.builder().s("UPD").build());
        exprAttrVal.put(":cond", AttributeValue.builder().n("1").build());
        uir.expressionAttributeValues(exprAttrVal);
        uir.returnValues(ReturnValue.UPDATED_NEW);
        UpdateItemResponse dynamoResult = dynamoDbClient.updateItem(uir.build());
        UpdateItemResponse phoenixResult = phoenixDBClientV2.updateItem(uir.build());
        Assert.assertEquals(dynamoResult.attributes(), phoenixResult.attributes());
        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testUpdatedNewConditionalFailureReturnsFullOldRow() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);
        Map<String, AttributeValue> key = getKey();

        UpdateItemRequest.Builder uir = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir.updateExpression("SET COL2 = :s");
        uir.conditionExpression("COL1 = :bogus");
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":s", AttributeValue.builder().s("UPD").build());
        exprAttrVal.put(":bogus", AttributeValue.builder().n("99999").build());
        uir.expressionAttributeValues(exprAttrVal);
        uir.returnValues(ReturnValue.UPDATED_NEW);
        uir.returnValuesOnConditionCheckFailure(ALL_OLD);

        Map<String, AttributeValue> dynamoExceptionItem = null;
        try {
            dynamoDbClient.updateItem(uir.build());
            Assert.fail("Expected ConditionalCheckFailedException from DDB");
        } catch (ConditionalCheckFailedException e) {
            Assert.assertEquals(400, e.statusCode());
            dynamoExceptionItem = e.item();
            Assert.assertNotNull("RVOCCF=ALL_OLD must surface the existing item",
                    dynamoExceptionItem);
        }
        try {
            phoenixDBClientV2.updateItem(uir.build());
            Assert.fail("Expected ConditionalCheckFailedException from Phoenix");
        } catch (ConditionalCheckFailedException e) {
            Assert.assertEquals(400, e.statusCode());
            Assert.assertEquals(dynamoExceptionItem, e.item());
        }

        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testUpdatedOldNewWithListAppend() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);
        Map<String, AttributeValue> key = getKey();

        // Seed MyList = [a, b].
        UpdateItemRequest.Builder seed = UpdateItemRequest.builder().tableName(tableName).key(key);
        seed.updateExpression("SET MyList = :init");
        Map<String, AttributeValue> seedVals = new HashMap<>();
        seedVals.put(":init", AttributeValue.builder().l(
                AttributeValue.builder().s("a").build(),
                AttributeValue.builder().s("b").build()).build());
        seed.expressionAttributeValues(seedVals);
        dynamoDbClient.updateItem(seed.build());
        phoenixDBClientV2.updateItem(seed.build());

        UpdateItemRequest.Builder uir1 = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir1.updateExpression("SET MyList = list_append(MyList, :extra)");
        Map<String, AttributeValue> exprAttrVal1 = new HashMap<>();
        exprAttrVal1.put(":extra", AttributeValue.builder().l(
                AttributeValue.builder().s("c").build()).build());
        uir1.expressionAttributeValues(exprAttrVal1);
        uir1.returnValues(ReturnValue.UPDATED_OLD);
        UpdateItemResponse dynamoResult1 = dynamoDbClient.updateItem(uir1.build());
        UpdateItemResponse phoenixResult1 = phoenixDBClientV2.updateItem(uir1.build());
        Assert.assertEquals(dynamoResult1.attributes(), phoenixResult1.attributes());

        // Reseed MyList = [a, b] so the NEW half starts from the same baseline.
        dynamoDbClient.updateItem(seed.build());
        phoenixDBClientV2.updateItem(seed.build());

        UpdateItemRequest.Builder uir2 = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir2.updateExpression("SET MyList = list_append(MyList, :extra)");
        Map<String, AttributeValue> exprAttrVal2 = new HashMap<>();
        exprAttrVal2.put(":extra", AttributeValue.builder().l(
                AttributeValue.builder().s("d").build(),
                AttributeValue.builder().s("e").build()).build());
        uir2.expressionAttributeValues(exprAttrVal2);
        uir2.returnValues(ReturnValue.UPDATED_NEW);
        UpdateItemResponse dynamoResult2 = dynamoDbClient.updateItem(uir2.build());
        UpdateItemResponse phoenixResult2 = phoenixDBClientV2.updateItem(uir2.build());
        Assert.assertEquals(dynamoResult2.attributes(), phoenixResult2.attributes());

        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testUpdatedOldNewIfNotExistsOnExistingAttr() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);
        Map<String, AttributeValue> key = getKey();
        // COL2 starts at "Title1" (see getItem1()).

        UpdateItemRequest.Builder uir1 = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir1.updateExpression("SET COL2 = if_not_exists(COL2, :fallback)");
        Map<String, AttributeValue> exprAttrVal1 = new HashMap<>();
        exprAttrVal1.put(":fallback", AttributeValue.builder().s("Default").build());
        uir1.expressionAttributeValues(exprAttrVal1);
        uir1.returnValues(ReturnValue.UPDATED_OLD);
        UpdateItemResponse dynamoResult1 = dynamoDbClient.updateItem(uir1.build());
        UpdateItemResponse phoenixResult1 = phoenixDBClientV2.updateItem(uir1.build());
        Assert.assertEquals(dynamoResult1.attributes(), phoenixResult1.attributes());

        UpdateItemRequest.Builder uir2 = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir2.updateExpression("SET COL2 = if_not_exists(COL2, :fallback)");
        Map<String, AttributeValue> exprAttrVal2 = new HashMap<>();
        exprAttrVal2.put(":fallback", AttributeValue.builder().s("Default").build());
        uir2.expressionAttributeValues(exprAttrVal2);
        uir2.returnValues(ReturnValue.UPDATED_NEW);
        UpdateItemResponse dynamoResult2 = dynamoDbClient.updateItem(uir2.build());
        UpdateItemResponse phoenixResult2 = phoenixDBClientV2.updateItem(uir2.build());
        Assert.assertEquals(dynamoResult2.attributes(), phoenixResult2.attributes());

        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testUpdatedOldNewIfNotExistsOnMissingAttr() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);
        Map<String, AttributeValue> key = getKey();
        // "Subhead" is not present in getItem1().

        UpdateItemRequest.Builder uir1 = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir1.updateExpression("SET Subhead = if_not_exists(Subhead, :fallback)");
        Map<String, AttributeValue> exprAttrVal1 = new HashMap<>();
        exprAttrVal1.put(":fallback", AttributeValue.builder().s("FirstWrite").build());
        uir1.expressionAttributeValues(exprAttrVal1);
        uir1.returnValues(ReturnValue.UPDATED_OLD);
        UpdateItemResponse dynamoResult1 = dynamoDbClient.updateItem(uir1.build());
        UpdateItemResponse phoenixResult1 = phoenixDBClientV2.updateItem(uir1.build());
        Assert.assertEquals(dynamoResult1.attributes(), phoenixResult1.attributes());

        // Reseed so the NEW half also runs against a row where Subhead does not yet exist.
        seedItem(tableName, getItem1());

        UpdateItemRequest.Builder uir2 = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir2.updateExpression("SET Subhead = if_not_exists(Subhead, :fallback)");
        Map<String, AttributeValue> exprAttrVal2 = new HashMap<>();
        exprAttrVal2.put(":fallback", AttributeValue.builder().s("FirstWrite").build());
        uir2.expressionAttributeValues(exprAttrVal2);
        uir2.returnValues(ReturnValue.UPDATED_NEW);
        UpdateItemResponse dynamoResult2 = dynamoDbClient.updateItem(uir2.build());
        UpdateItemResponse phoenixResult2 = phoenixDBClientV2.updateItem(uir2.build());
        Assert.assertEquals(dynamoResult2.attributes(), phoenixResult2.attributes());

        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testUpdatedOldNewWithArithmetic() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);
        Map<String, AttributeValue> key = getKey();
        // COL1 starts at 1, COL4 starts at 34.

        UpdateItemRequest.Builder uir1 = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir1.updateExpression("SET COL1 = COL1 + :a, COL4 = COL4 - :b");
        Map<String, AttributeValue> exprAttrVal1 = new HashMap<>();
        exprAttrVal1.put(":a", AttributeValue.builder().n("3").build());
        exprAttrVal1.put(":b", AttributeValue.builder().n("4").build());
        uir1.expressionAttributeValues(exprAttrVal1);
        uir1.returnValues(ReturnValue.UPDATED_OLD);
        UpdateItemResponse dynamoResult1 = dynamoDbClient.updateItem(uir1.build());
        UpdateItemResponse phoenixResult1 = phoenixDBClientV2.updateItem(uir1.build());
        Assert.assertEquals(dynamoResult1.attributes(), phoenixResult1.attributes());

        seedItem(tableName, getItem1());

        UpdateItemRequest.Builder uir2 = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir2.updateExpression("SET COL1 = COL1 + :a, COL4 = COL4 - :b");
        Map<String, AttributeValue> exprAttrVal2 = new HashMap<>();
        exprAttrVal2.put(":a", AttributeValue.builder().n("3").build());
        exprAttrVal2.put(":b", AttributeValue.builder().n("4").build());
        uir2.expressionAttributeValues(exprAttrVal2);
        uir2.returnValues(ReturnValue.UPDATED_NEW);
        UpdateItemResponse dynamoResult2 = dynamoDbClient.updateItem(uir2.build());
        UpdateItemResponse phoenixResult2 = phoenixDBClientV2.updateItem(uir2.build());
        Assert.assertEquals(dynamoResult2.attributes(), phoenixResult2.attributes());

        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testUpdatedOldNewSiblingNestedPaths() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);
        Map<String, AttributeValue> key = getKey();

        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#r", "Reviews");
        exprAttrNames.put("#f", "FiveStar");
        exprAttrNames.put("#rv", "reviewer");
        exprAttrNames.put("#cmt", "comment");

        UpdateItemRequest.Builder uir1 = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir1.updateExpression("SET #r.#f[0].#rv = :rv, #r.#f[0].#cmt = :cmt");
        uir1.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal1 = new HashMap<>();
        exprAttrVal1.put(":rv", AttributeValue.builder().s("Carol").build());
        exprAttrVal1.put(":cmt", AttributeValue.builder().s("Great!").build());
        uir1.expressionAttributeValues(exprAttrVal1);
        uir1.returnValues(ReturnValue.UPDATED_OLD);
        UpdateItemResponse dynamoResult1 = dynamoDbClient.updateItem(uir1.build());
        UpdateItemResponse phoenixResult1 = phoenixDBClientV2.updateItem(uir1.build());
        Assert.assertEquals(dynamoResult1.attributes(), phoenixResult1.attributes());

        UpdateItemRequest.Builder uir2 = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir2.updateExpression("SET #r.#f[0].#rv = :rv, #r.#f[0].#cmt = :cmt");
        uir2.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal2 = new HashMap<>();
        exprAttrVal2.put(":rv", AttributeValue.builder().s("Dave").build());
        exprAttrVal2.put(":cmt", AttributeValue.builder().s("Excellent").build());
        uir2.expressionAttributeValues(exprAttrVal2);
        uir2.returnValues(ReturnValue.UPDATED_NEW);
        UpdateItemResponse dynamoResult2 = dynamoDbClient.updateItem(uir2.build());
        UpdateItemResponse phoenixResult2 = phoenixDBClientV2.updateItem(uir2.build());
        Assert.assertEquals(dynamoResult2.attributes(), phoenixResult2.attributes());

        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testUpdatedOldNewWithDeleteFromSet() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);
        Map<String, AttributeValue> key = getKey();

        // Seed a multi-element string set so a DELETE leaves the set non-empty.
        UpdateItemRequest.Builder seed = UpdateItemRequest.builder().tableName(tableName).key(key);
        seed.updateExpression("SET TopLevelSet = :ss");
        Map<String, AttributeValue> seedVals = new HashMap<>();
        seedVals.put(":ss", AttributeValue.builder().ss("a", "b", "c").build());
        seed.expressionAttributeValues(seedVals);
        dynamoDbClient.updateItem(seed.build());
        phoenixDBClientV2.updateItem(seed.build());

        UpdateItemRequest.Builder uir1 = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir1.updateExpression("DELETE TopLevelSet :r");
        Map<String, AttributeValue> exprAttrVal1 = new HashMap<>();
        exprAttrVal1.put(":r", AttributeValue.builder().ss("b").build());
        uir1.expressionAttributeValues(exprAttrVal1);
        uir1.returnValues(ReturnValue.UPDATED_OLD);
        UpdateItemResponse dynamoResult1 = dynamoDbClient.updateItem(uir1.build());
        UpdateItemResponse phoenixResult1 = phoenixDBClientV2.updateItem(uir1.build());
        Assert.assertEquals(dynamoResult1.attributes(), phoenixResult1.attributes());

        // Reseed the set for the NEW half.
        dynamoDbClient.updateItem(seed.build());
        phoenixDBClientV2.updateItem(seed.build());

        UpdateItemRequest.Builder uir2 = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir2.updateExpression("DELETE TopLevelSet :r");
        Map<String, AttributeValue> exprAttrVal2 = new HashMap<>();
        exprAttrVal2.put(":r", AttributeValue.builder().ss("c").build());
        uir2.expressionAttributeValues(exprAttrVal2);
        uir2.returnValues(ReturnValue.UPDATED_NEW);
        UpdateItemResponse dynamoResult2 = dynamoDbClient.updateItem(uir2.build());
        UpdateItemResponse phoenixResult2 = phoenixDBClientV2.updateItem(uir2.build());
        Assert.assertEquals(dynamoResult2.attributes(), phoenixResult2.attributes());

        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testUpdatedOldNewWithAddStringSet() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);
        Map<String, AttributeValue> key = getKey();
        // TopLevelSet starts as ss("setMember1") in getItem1().

        UpdateItemRequest.Builder uir1 = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir1.updateExpression("ADD TopLevelSet :a");
        Map<String, AttributeValue> exprAttrVal1 = new HashMap<>();
        exprAttrVal1.put(":a", AttributeValue.builder().ss("setMember2").build());
        uir1.expressionAttributeValues(exprAttrVal1);
        uir1.returnValues(ReturnValue.UPDATED_OLD);
        UpdateItemResponse dynamoResult1 = dynamoDbClient.updateItem(uir1.build());
        UpdateItemResponse phoenixResult1 = phoenixDBClientV2.updateItem(uir1.build());
        Assert.assertEquals(dynamoResult1.attributes(), phoenixResult1.attributes());

        seedItem(tableName, getItem1());

        UpdateItemRequest.Builder uir2 = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir2.updateExpression("ADD TopLevelSet :a");
        Map<String, AttributeValue> exprAttrVal2 = new HashMap<>();
        exprAttrVal2.put(":a", AttributeValue.builder().ss("setMember3").build());
        uir2.expressionAttributeValues(exprAttrVal2);
        uir2.returnValues(ReturnValue.UPDATED_NEW);
        UpdateItemResponse dynamoResult2 = dynamoDbClient.updateItem(uir2.build());
        UpdateItemResponse phoenixResult2 = phoenixDBClientV2.updateItem(uir2.build());
        Assert.assertEquals(dynamoResult2.attributes(), phoenixResult2.attributes());

        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testUpdatedOldNewAllFourClauses() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);
        Map<String, AttributeValue> key = getKey();

        // Seed TopLevelSet with multiple elements so DELETE doesn't empty it.
        UpdateItemRequest.Builder seed = UpdateItemRequest.builder().tableName(tableName).key(key);
        seed.updateExpression("SET TopLevelSet = :ss");
        Map<String, AttributeValue> seedVals = new HashMap<>();
        seedVals.put(":ss", AttributeValue.builder().ss("x", "y", "z").build());
        seed.expressionAttributeValues(seedVals);
        dynamoDbClient.updateItem(seed.build());
        phoenixDBClientV2.updateItem(seed.build());

        UpdateItemRequest.Builder uir1 = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir1.updateExpression("SET COL2 = :s REMOVE COL3 ADD COL1 :n DELETE TopLevelSet :rm");
        Map<String, AttributeValue> exprAttrVal1 = new HashMap<>();
        exprAttrVal1.put(":s", AttributeValue.builder().s("UPD").build());
        exprAttrVal1.put(":n", AttributeValue.builder().n("100").build());
        exprAttrVal1.put(":rm", AttributeValue.builder().ss("y").build());
        uir1.expressionAttributeValues(exprAttrVal1);
        uir1.returnValues(ReturnValue.UPDATED_OLD);
        UpdateItemResponse dynamoResult1 = dynamoDbClient.updateItem(uir1.build());
        UpdateItemResponse phoenixResult1 = phoenixDBClientV2.updateItem(uir1.build());
        Assert.assertEquals(dynamoResult1.attributes(), phoenixResult1.attributes());

        // Reseed (including TopLevelSet) for the NEW half.
        seedItem(tableName, getItem1());
        dynamoDbClient.updateItem(seed.build());
        phoenixDBClientV2.updateItem(seed.build());

        UpdateItemRequest.Builder uir2 = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir2.updateExpression("SET COL2 = :s REMOVE COL3 ADD COL1 :n DELETE TopLevelSet :rm");
        Map<String, AttributeValue> exprAttrVal2 = new HashMap<>();
        exprAttrVal2.put(":s", AttributeValue.builder().s("UPD2").build());
        exprAttrVal2.put(":n", AttributeValue.builder().n("50").build());
        exprAttrVal2.put(":rm", AttributeValue.builder().ss("z").build());
        uir2.expressionAttributeValues(exprAttrVal2);
        uir2.returnValues(ReturnValue.UPDATED_NEW);
        UpdateItemResponse dynamoResult2 = dynamoDbClient.updateItem(uir2.build());
        UpdateItemResponse phoenixResult2 = phoenixDBClientV2.updateItem(uir2.build());
        Assert.assertEquals(dynamoResult2.attributes(), phoenixResult2.attributes());

        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testUpdatedOldWhenSetCreatesNewAttribute() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);
        Map<String, AttributeValue> key = getKey();

        UpdateItemRequest.Builder uir1 = UpdateItemRequest.builder().tableName(tableName).key(key);
        // "BrandNewField" is not present in getItem1().
        uir1.updateExpression("SET BrandNewField = :v");
        Map<String, AttributeValue> exprAttrVal1 = new HashMap<>();
        exprAttrVal1.put(":v", AttributeValue.builder().s("hello").build());
        uir1.expressionAttributeValues(exprAttrVal1);
        uir1.returnValues(ReturnValue.UPDATED_OLD);
        UpdateItemResponse dynamoResult1 = dynamoDbClient.updateItem(uir1.build());
        UpdateItemResponse phoenixResult1 = phoenixDBClientV2.updateItem(uir1.build());
        Assert.assertEquals(dynamoResult1.attributes(), phoenixResult1.attributes());

        UpdateItemRequest.Builder uir2 = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir2.updateExpression("SET AnotherNew = :v");
        Map<String, AttributeValue> exprAttrVal2 = new HashMap<>();
        exprAttrVal2.put(":v", AttributeValue.builder().s("world").build());
        uir2.expressionAttributeValues(exprAttrVal2);
        uir2.returnValues(ReturnValue.UPDATED_NEW);
        UpdateItemResponse dynamoResult2 = dynamoDbClient.updateItem(uir2.build());
        UpdateItemResponse phoenixResult2 = phoenixDBClientV2.updateItem(uir2.build());
        Assert.assertEquals(dynamoResult2.attributes(), phoenixResult2.attributes());

        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testUpdatedOldConditionalSuccess() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);
        Map<String, AttributeValue> key = getKey();

        UpdateItemRequest.Builder uir = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir.updateExpression("SET COL2 = :s");
        uir.conditionExpression("COL1 = :cond");
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":s", AttributeValue.builder().s("UPD").build());
        exprAttrVal.put(":cond", AttributeValue.builder().n("1").build());
        uir.expressionAttributeValues(exprAttrVal);
        uir.returnValues(ReturnValue.UPDATED_OLD);
        UpdateItemResponse dynamoResult = dynamoDbClient.updateItem(uir.build());
        UpdateItemResponse phoenixResult = phoenixDBClientV2.updateItem(uir.build());
        Assert.assertEquals(dynamoResult.attributes(), phoenixResult.attributes());
        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testUpdatedNewWithRvoccfAllOldOnSuccess() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);
        Map<String, AttributeValue> key = getKey();

        UpdateItemRequest.Builder uir = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir.updateExpression("SET COL2 = :s");
        uir.conditionExpression("COL1 = :cond");
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":s", AttributeValue.builder().s("UPD").build());
        exprAttrVal.put(":cond", AttributeValue.builder().n("1").build());
        uir.expressionAttributeValues(exprAttrVal);
        uir.returnValues(ReturnValue.UPDATED_NEW);
        uir.returnValuesOnConditionCheckFailure(ALL_OLD);
        UpdateItemResponse dynamoResult = dynamoDbClient.updateItem(uir.build());
        UpdateItemResponse phoenixResult = phoenixDBClientV2.updateItem(uir.build());
        Assert.assertEquals(dynamoResult.attributes(), phoenixResult.attributes());
        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testUpdatedOldWithRvoccfAllOldOnSuccess() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);
        Map<String, AttributeValue> key = getKey();

        UpdateItemRequest.Builder uir = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir.updateExpression("SET COL2 = :s");
        uir.conditionExpression("COL1 = :cond");
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":s", AttributeValue.builder().s("UPD").build());
        exprAttrVal.put(":cond", AttributeValue.builder().n("1").build());
        uir.expressionAttributeValues(exprAttrVal);
        uir.returnValues(ReturnValue.UPDATED_OLD);
        uir.returnValuesOnConditionCheckFailure(ALL_OLD);
        UpdateItemResponse dynamoResult = dynamoDbClient.updateItem(uir.build());
        UpdateItemResponse phoenixResult = phoenixDBClientV2.updateItem(uir.build());
        Assert.assertEquals(dynamoResult.attributes(), phoenixResult.attributes());
        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testUpdatedOldNewLegacyAttributeUpdatesDelete() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);
        Map<String, AttributeValue> key = getKey();

        // Seed a multi-element string set so DELETE-with-value leaves the set non-empty.
        UpdateItemRequest.Builder seed = UpdateItemRequest.builder().tableName(tableName).key(key);
        seed.updateExpression("SET TopLevelSet = :ss");
        Map<String, AttributeValue> seedVals = new HashMap<>();
        seedVals.put(":ss", AttributeValue.builder().ss("a", "b", "c").build());
        seed.expressionAttributeValues(seedVals);
        dynamoDbClient.updateItem(seed.build());
        phoenixDBClientV2.updateItem(seed.build());

        Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate> updates =
                new HashMap<>();
        // DELETE-without-value -> REMOVE COL3
        updates.put("COL3", software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate
                .builder()
                .action(software.amazon.awssdk.services.dynamodb.model.AttributeAction.DELETE)
                .build());
        // DELETE-with-value -> DELETE_FROM_SET TopLevelSet :v
        updates.put("TopLevelSet", software.amazon.awssdk.services.dynamodb.model
                .AttributeValueUpdate.builder()
                .action(software.amazon.awssdk.services.dynamodb.model.AttributeAction.DELETE)
                .value(AttributeValue.builder().ss("b").build()).build());

        UpdateItemRequest.Builder uir1 = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir1.attributeUpdates(updates);
        uir1.returnValues(ReturnValue.UPDATED_OLD);
        UpdateItemResponse dynamoResult1 = dynamoDbClient.updateItem(uir1.build());
        UpdateItemResponse phoenixResult1 = phoenixDBClientV2.updateItem(uir1.build());
        Assert.assertEquals(dynamoResult1.attributes(), phoenixResult1.attributes());

        seedItem(tableName, getItem1());
        dynamoDbClient.updateItem(seed.build());
        phoenixDBClientV2.updateItem(seed.build());

        UpdateItemRequest.Builder uir2 = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir2.attributeUpdates(updates);
        uir2.returnValues(ReturnValue.UPDATED_NEW);
        UpdateItemResponse dynamoResult2 = dynamoDbClient.updateItem(uir2.build());
        UpdateItemResponse phoenixResult2 = phoenixDBClientV2.updateItem(uir2.build());
        Assert.assertEquals(dynamoResult2.attributes(), phoenixResult2.attributes());

        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testUpdatedOldNewWithLegacyExpected() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);
        Map<String, AttributeValue> key = getKey();

        Map<String, software.amazon.awssdk.services.dynamodb.model.ExpectedAttributeValue> expected =
                new HashMap<>();
        expected.put("COL1", software.amazon.awssdk.services.dynamodb.model.ExpectedAttributeValue
                .builder()
                .comparisonOperator(
                        software.amazon.awssdk.services.dynamodb.model.ComparisonOperator.EQ)
                .attributeValueList(AttributeValue.builder().n("1").build()).build());

        Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate> updates =
                new HashMap<>();
        updates.put("COL2", software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate
                .builder()
                .action(software.amazon.awssdk.services.dynamodb.model.AttributeAction.PUT)
                .value(AttributeValue.builder().s("UPD").build()).build());

        UpdateItemRequest.Builder uir1 = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir1.attributeUpdates(updates);
        uir1.expected(expected);
        uir1.returnValues(ReturnValue.UPDATED_OLD);
        UpdateItemResponse dynamoResult1 = dynamoDbClient.updateItem(uir1.build());
        UpdateItemResponse phoenixResult1 = phoenixDBClientV2.updateItem(uir1.build());
        Assert.assertEquals(dynamoResult1.attributes(), phoenixResult1.attributes());

        seedItem(tableName, getItem1());

        UpdateItemRequest.Builder uir2 = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir2.attributeUpdates(updates);
        uir2.expected(expected);
        uir2.returnValues(ReturnValue.UPDATED_NEW);
        UpdateItemResponse dynamoResult2 = dynamoDbClient.updateItem(uir2.build());
        UpdateItemResponse phoenixResult2 = phoenixDBClientV2.updateItem(uir2.build());
        Assert.assertEquals(dynamoResult2.attributes(), phoenixResult2.attributes());

        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testConcurrentConditionalUpdateWithUpdatedNew() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);

        UpdateItemRequest.Builder uir =
                UpdateItemRequest.builder().tableName(tableName).key(getKey());
        uir.updateExpression("SET #1 = #1 + :v1");
        uir.conditionExpression("#1 < :condVal");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "COL1");
        uir.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v1", AttributeValue.builder().n("10").build());
        exprAttrVal.put(":condVal", AttributeValue.builder().n("5").build());
        uir.expressionAttributeValues(exprAttrVal);
        uir.returnValues(ReturnValue.UPDATED_NEW);
        uir.returnValuesOnConditionCheckFailure(ALL_OLD);

        // Pre-condition DDB so subsequent updates fail their condition. Captured payload is the
        // expected winning UPDATED_NEW projection ({COL1: {N: "11"}}).
        Map<String, AttributeValue> newItem = dynamoDbClient.updateItem(uir.build()).attributes();

        ExecutorService executorService = Executors.newFixedThreadPool(5);
        AtomicInteger updateCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);

        for (int i = 0; i < 5; i++) {
            executorService.submit(() -> {
                Map<String, AttributeValue> oldItem = null;
                try {
                    dynamoDbClient.updateItem(uir.build());
                } catch (ConditionalCheckFailedException e) {
                    oldItem = e.item();
                }
                try {
                    UpdateItemResponse result = phoenixDBClientV2.updateItem(uir.build());
                    Assert.assertEquals(newItem, result.attributes());
                    updateCount.incrementAndGet();
                } catch (ConditionalCheckFailedException e) {
                    Assert.assertEquals(oldItem, e.item());
                    errorCount.incrementAndGet();
                }
            });
        }
        executorService.shutdown();
        try {
            boolean terminated = executorService.awaitTermination(30, TimeUnit.SECONDS);
            if (terminated) {
                Assert.assertEquals(1, updateCount.get());
                Assert.assertEquals(4, errorCount.get());
            } else {
                Assert.fail(
                        "testConcurrentConditionalUpdateWithUpdatedNew: threads did not terminate.");
            }
        } catch (InterruptedException e) {
            Assert.fail("testConcurrentConditionalUpdateWithUpdatedNew was interrupted.");
        }
    }

    @Test(timeout = 120000)
    public void testUpdatedNewMultiType() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);
        Map<String, AttributeValue> key = getKey();

        Map<String, AttributeValue> nested = new HashMap<>();
        nested.put("inner", AttributeValue.builder().s("deep").build());

        UpdateItemRequest.Builder uir = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir.updateExpression("SET strAttr = :s, numAttr = :n, binAttr = :b, boolAttr = :bo, "
                + "nullAttr = :nu, listAttr = :l, mapAttr = :m, ssAttr = :ss, nsAttr = :ns, "
                + "bsAttr = :bs");
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":s", AttributeValue.builder().s("hello").build());
        exprAttrVal.put(":n", AttributeValue.builder().n("42").build());
        exprAttrVal.put(":b", AttributeValue.builder()
                .b(SdkBytes.fromByteArray(new byte[] {1, 2, 3})).build());
        exprAttrVal.put(":bo", AttributeValue.builder().bool(true).build());
        exprAttrVal.put(":nu", AttributeValue.builder().nul(true).build());
        exprAttrVal.put(":l", AttributeValue.builder().l(
                AttributeValue.builder().s("a").build(),
                AttributeValue.builder().n("1").build()).build());
        exprAttrVal.put(":m", AttributeValue.builder().m(nested).build());
        exprAttrVal.put(":ss", AttributeValue.builder().ss("x", "y").build());
        exprAttrVal.put(":ns", AttributeValue.builder().ns("1", "2", "3").build());
        exprAttrVal.put(":bs", AttributeValue.builder().bs(
                SdkBytes.fromByteArray(new byte[] {4}),
                SdkBytes.fromByteArray(new byte[] {5})).build());
        uir.expressionAttributeValues(exprAttrVal);
        uir.returnValues(ReturnValue.UPDATED_NEW);
        UpdateItemResponse dynamoResult = dynamoDbClient.updateItem(uir.build());
        UpdateItemResponse phoenixResult = phoenixDBClientV2.updateItem(uir.build());
        Assert.assertEquals(dynamoResult.attributes(), phoenixResult.attributes());
        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testUpdatedOldNewLegacyAttributeUpdates() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);
        Map<String, AttributeValue> key = getKey();

        Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate> updates =
                new HashMap<>();
        updates.put("COL2", software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate
                .builder()
                .action(software.amazon.awssdk.services.dynamodb.model.AttributeAction.PUT)
                .value(AttributeValue.builder().s("LegacyTitle").build()).build());
        updates.put("Counter", software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate
                .builder()
                .action(software.amazon.awssdk.services.dynamodb.model.AttributeAction.ADD)
                .value(AttributeValue.builder().n("7").build()).build());

        UpdateItemRequest.Builder uir1 = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir1.attributeUpdates(updates);
        uir1.returnValues(ReturnValue.UPDATED_OLD);
        UpdateItemResponse dynamoResult1 = dynamoDbClient.updateItem(uir1.build());
        UpdateItemResponse phoenixResult1 = phoenixDBClientV2.updateItem(uir1.build());
        Assert.assertEquals(dynamoResult1.attributes(), phoenixResult1.attributes());

        seedItem(tableName, getItem1());

        UpdateItemRequest.Builder uir2 = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir2.attributeUpdates(updates);
        uir2.returnValues(ReturnValue.UPDATED_NEW);
        UpdateItemResponse dynamoResult2 = dynamoDbClient.updateItem(uir2.build());
        UpdateItemResponse phoenixResult2 = phoenixDBClientV2.updateItem(uir2.build());
        Assert.assertEquals(dynamoResult2.attributes(), phoenixResult2.attributes());

        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testUpdatedNewMapFieldUpdate() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);
        Map<String, AttributeValue> key = getKey();

        // Seed MyMap with three sibling fields so the multi-field UPDATE can leave one untouched.
        Map<String, AttributeValue> seedMyMap = new HashMap<>();
        seedMyMap.put("field1", AttributeValue.builder().s("orig1").build());
        seedMyMap.put("field2", AttributeValue.builder().s("orig2").build());
        seedMyMap.put("field3", AttributeValue.builder().s("orig3").build());
        UpdateItemRequest.Builder seed = UpdateItemRequest.builder().tableName(tableName).key(key);
        seed.updateExpression("SET MyMap = :init");
        Map<String, AttributeValue> seedVals = new HashMap<>();
        seedVals.put(":init", AttributeValue.builder().m(seedMyMap).build());
        seed.expressionAttributeValues(seedVals);
        dynamoDbClient.updateItem(seed.build());
        phoenixDBClientV2.updateItem(seed.build());

        UpdateItemRequest.Builder uir = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir.updateExpression("SET MyMap.field2 = :v1, MyMap.field3 = :v2");
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v1", AttributeValue.builder().s("UPDATED-2").build());
        exprAttrVal.put(":v2", AttributeValue.builder().s("UPDATED-3").build());
        uir.expressionAttributeValues(exprAttrVal);
        uir.returnValues(ReturnValue.UPDATED_NEW);
        UpdateItemResponse dynamoResult = dynamoDbClient.updateItem(uir.build());
        UpdateItemResponse phoenixResult = phoenixDBClientV2.updateItem(uir.build());
        Assert.assertEquals(dynamoResult.attributes(), phoenixResult.attributes());

        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testUpdatedNewDeepMapFieldUpdate() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);
        Map<String, AttributeValue> key = getKey();

        // Seed WrapperMap.middle with three siblings and WrapperMap.otherTop as a top-level
        // sibling so we can touch both branches and leave one leaf untouched.
        Map<String, AttributeValue> seedMiddle = new HashMap<>();
        seedMiddle.put("leaf1", AttributeValue.builder().s("a").build());
        seedMiddle.put("leaf2", AttributeValue.builder().s("b").build());
        seedMiddle.put("leaf3", AttributeValue.builder().s("c").build());
        Map<String, AttributeValue> seedWrapper = new HashMap<>();
        seedWrapper.put("middle", AttributeValue.builder().m(seedMiddle).build());
        seedWrapper.put("otherTop", AttributeValue.builder().s("y").build());
        UpdateItemRequest.Builder seed = UpdateItemRequest.builder().tableName(tableName).key(key);
        seed.updateExpression("SET WrapperMap = :init");
        Map<String, AttributeValue> seedVals = new HashMap<>();
        seedVals.put(":init", AttributeValue.builder().m(seedWrapper).build());
        seed.expressionAttributeValues(seedVals);
        dynamoDbClient.updateItem(seed.build());
        phoenixDBClientV2.updateItem(seed.build());

        UpdateItemRequest.Builder uir = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir.updateExpression(
            "SET WrapperMap.middle.leaf2 = :v1, WrapperMap.middle.leaf3 = :v2, "
                + "WrapperMap.otherTop = :v3");
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v1", AttributeValue.builder().s("UPDATED-leaf2").build());
        exprAttrVal.put(":v2", AttributeValue.builder().s("UPDATED-leaf3").build());
        exprAttrVal.put(":v3", AttributeValue.builder().s("UPDATED-other").build());
        uir.expressionAttributeValues(exprAttrVal);
        uir.returnValues(ReturnValue.UPDATED_NEW);
        UpdateItemResponse dynamoResult = dynamoDbClient.updateItem(uir.build());
        UpdateItemResponse phoenixResult = phoenixDBClientV2.updateItem(uir.build());
        Assert.assertEquals(dynamoResult.attributes(), phoenixResult.attributes());

        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testUpdatedNewMixedDotBracketPath() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);
        Map<String, AttributeValue> key = getKey();

        // Build MyHolder.subEntries = [ [map, map, map, map], [map, map, map, map],
        //                               [map, map, map, map] ]
        // Each leaf map has a single field `subField` with a positional value.
        AttributeValue[] outer = new AttributeValue[3];
        for (int i = 0; i < outer.length; i++) {
            AttributeValue[] inner = new AttributeValue[4];
            for (int j = 0; j < inner.length; j++) {
                Map<String, AttributeValue> leaf = new HashMap<>();
                leaf.put("subField", AttributeValue.builder().s("orig-" + i + "-" + j).build());
                inner[j] = AttributeValue.builder().m(leaf).build();
            }
            outer[i] = AttributeValue.builder().l(inner).build();
        }
        Map<String, AttributeValue> seedHolder = new HashMap<>();
        seedHolder.put("subEntries", AttributeValue.builder().l(outer).build());
        seedHolder.put("otherTop", AttributeValue.builder().s("untouched").build());

        // DDB validates that every ExpressionAttributeNames key is actually referenced in the
        // expression — so the seed (which only mentions #h) needs its own minimal map.
        Map<String, String> seedAttrNames = new HashMap<>();
        seedAttrNames.put("#h", "MyHolder");

        UpdateItemRequest.Builder seed = UpdateItemRequest.builder().tableName(tableName).key(key);
        seed.updateExpression("SET #h = :init");
        seed.expressionAttributeNames(seedAttrNames);
        Map<String, AttributeValue> seedVals = new HashMap<>();
        seedVals.put(":init", AttributeValue.builder().m(seedHolder).build());
        seed.expressionAttributeValues(seedVals);
        dynamoDbClient.updateItem(seed.build());
        phoenixDBClientV2.updateItem(seed.build());

        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#h", "MyHolder");
        exprAttrNames.put("#e", "subEntries");
        exprAttrNames.put("#sf", "subField");

        UpdateItemRequest.Builder uir = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir.updateExpression("SET #h.#e[1][3].#sf = :v1, #h.#e[2][0].#sf = :v2");
        uir.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v1", AttributeValue.builder().s("UPDATED-1-3").build());
        exprAttrVal.put(":v2", AttributeValue.builder().s("UPDATED-2-0").build());
        uir.expressionAttributeValues(exprAttrVal);
        uir.returnValues(ReturnValue.UPDATED_NEW);
        UpdateItemResponse dynamoResult = dynamoDbClient.updateItem(uir.build());
        UpdateItemResponse phoenixResult = phoenixDBClientV2.updateItem(uir.build());
        Assert.assertEquals(dynamoResult.attributes(), phoenixResult.attributes());

        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testUpdatedNewListIndexNonZero() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);
        Map<String, AttributeValue> key = getKey();

        // Seed MyList with 6 maps so indices 2 and 4 both exist with unchanged neighbors.
        AttributeValue[] initList = new AttributeValue[6];
        for (int i = 0; i < initList.length; i++) {
            Map<String, AttributeValue> el = new HashMap<>();
            el.put("sku", AttributeValue.builder().s("orig" + i).build());
            initList[i] = AttributeValue.builder().m(el).build();
        }
        UpdateItemRequest.Builder seed = UpdateItemRequest.builder().tableName(tableName).key(key);
        seed.updateExpression("SET MyList = :init");
        Map<String, AttributeValue> seedVals = new HashMap<>();
        seedVals.put(":init", AttributeValue.builder().l(initList).build());
        seed.expressionAttributeValues(seedVals);
        dynamoDbClient.updateItem(seed.build());
        phoenixDBClientV2.updateItem(seed.build());

        UpdateItemRequest.Builder uir = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir.updateExpression("SET MyList[2].sku = :v1, MyList[4].sku = :v2");
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v1", AttributeValue.builder().s("UPDATED-2").build());
        exprAttrVal.put(":v2", AttributeValue.builder().s("UPDATED-4").build());
        uir.expressionAttributeValues(exprAttrVal);
        uir.returnValues(ReturnValue.UPDATED_NEW);
        UpdateItemResponse dynamoResult = dynamoDbClient.updateItem(uir.build());
        UpdateItemResponse phoenixResult = phoenixDBClientV2.updateItem(uir.build());
        Assert.assertEquals(dynamoResult.attributes(), phoenixResult.attributes());

        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testUpdatedOldNewRemoveListIndex() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);
        Map<String, AttributeValue> key = getKey();

        UpdateItemRequest.Builder seed = UpdateItemRequest.builder().tableName(tableName).key(key);
        seed.updateExpression("SET MyList = :init");
        Map<String, AttributeValue> seedVals = new HashMap<>();
        seedVals.put(":init", AttributeValue.builder().l(
            AttributeValue.builder().s("a").build(),
            AttributeValue.builder().s("b").build(),
            AttributeValue.builder().s("c").build(),
            AttributeValue.builder().s("d").build(),
            AttributeValue.builder().s("e").build()).build());
        seed.expressionAttributeValues(seedVals);
        seed.returnValues(ReturnValue.UPDATED_OLD);
        UpdateItemResponse dynamoSeed1 = dynamoDbClient.updateItem(seed.build());
        UpdateItemResponse phoenixSeed1 = phoenixDBClientV2.updateItem(seed.build());
        Assert.assertEquals(dynamoSeed1.attributes(), phoenixSeed1.attributes());

        UpdateItemRequest.Builder uir1 = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir1.updateExpression("REMOVE MyList[2]");
        uir1.returnValues(ReturnValue.UPDATED_OLD);
        UpdateItemResponse dynamoResult1 = dynamoDbClient.updateItem(uir1.build());
        UpdateItemResponse phoenixResult1 = phoenixDBClientV2.updateItem(uir1.build());
        Assert.assertEquals(dynamoResult1.attributes(), phoenixResult1.attributes());

        UpdateItemResponse dynamoSeed2 = dynamoDbClient.updateItem(seed.build());
        UpdateItemResponse phoenixSeed2 = phoenixDBClientV2.updateItem(seed.build());
        Assert.assertEquals(dynamoSeed2.attributes(), phoenixSeed2.attributes());

        UpdateItemRequest.Builder uir2 = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir2.updateExpression("REMOVE MyList[2]");
        uir2.returnValues(ReturnValue.UPDATED_NEW);
        UpdateItemResponse dynamoResult2 = dynamoDbClient.updateItem(uir2.build());
        UpdateItemResponse phoenixResult2 = phoenixDBClientV2.updateItem(uir2.build());
        Assert.assertEquals(dynamoResult2.attributes(), phoenixResult2.attributes());

        dynamoDbClient.updateItem(seed.build());
        phoenixDBClientV2.updateItem(seed.build());

        UpdateItemRequest.Builder uir3 = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir3.updateExpression("REMOVE MyList[0]");
        uir3.returnValues(ReturnValue.UPDATED_OLD);
        UpdateItemResponse dynamoResult3 = dynamoDbClient.updateItem(uir3.build());
        UpdateItemResponse phoenixResult3 = phoenixDBClientV2.updateItem(uir3.build());
        Assert.assertEquals(dynamoResult3.attributes(), phoenixResult3.attributes());

        dynamoDbClient.updateItem(seed.build());
        phoenixDBClientV2.updateItem(seed.build());

        UpdateItemRequest.Builder uir4 = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir4.updateExpression("REMOVE MyList[4]");
        uir4.returnValues(ReturnValue.UPDATED_NEW);
        UpdateItemResponse dynamoResult4 = dynamoDbClient.updateItem(uir4.build());
        UpdateItemResponse phoenixResult4 = phoenixDBClientV2.updateItem(uir4.build());
        Assert.assertEquals(dynamoResult4.attributes(), phoenixResult4.attributes());

        dynamoDbClient.updateItem(seed.build());
        phoenixDBClientV2.updateItem(seed.build());

        UpdateItemRequest.Builder uir5 = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir5.updateExpression("REMOVE MyList[0]");
        uir5.returnValues(ReturnValue.UPDATED_NEW);
        UpdateItemResponse dynamoResult5 = dynamoDbClient.updateItem(uir5.build());
        UpdateItemResponse phoenixResult5 = phoenixDBClientV2.updateItem(uir5.build());
        Assert.assertEquals(dynamoResult5.attributes(), phoenixResult5.attributes());

        dynamoDbClient.updateItem(seed.build());
        phoenixDBClientV2.updateItem(seed.build());

        UpdateItemRequest.Builder uir6 = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir6.updateExpression("REMOVE MyList[4]");
        uir6.returnValues(ReturnValue.UPDATED_OLD);
        UpdateItemResponse dynamoResult6 = dynamoDbClient.updateItem(uir6.build());
        UpdateItemResponse phoenixResult6 = phoenixDBClientV2.updateItem(uir6.build());
        Assert.assertEquals(dynamoResult6.attributes(), phoenixResult6.attributes());

        dynamoDbClient.updateItem(seed.build());
        phoenixDBClientV2.updateItem(seed.build());

        UpdateItemRequest.Builder uir7 = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir7.updateExpression("REMOVE MyList[4]");
        uir7.returnValues(ReturnValue.ALL_OLD);
        UpdateItemResponse dynamoResult7 = dynamoDbClient.updateItem(uir7.build());
        UpdateItemResponse phoenixResult7 = phoenixDBClientV2.updateItem(uir7.build());
        Assert.assertEquals(dynamoResult7.attributes(), phoenixResult7.attributes());

        dynamoDbClient.updateItem(seed.build());
        phoenixDBClientV2.updateItem(seed.build());

        UpdateItemRequest.Builder uir8 = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir8.updateExpression("REMOVE MyList[4]");
        uir8.returnValues(ReturnValue.ALL_NEW);
        UpdateItemResponse dynamoResult8 = dynamoDbClient.updateItem(uir8.build());
        UpdateItemResponse phoenixResult8 = phoenixDBClientV2.updateItem(uir8.build());
        Assert.assertEquals(dynamoResult8.attributes(), phoenixResult8.attributes());

        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testUpdatedOldNewRemoveNestedMapField() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);
        Map<String, AttributeValue> key = getKey();

        Map<String, AttributeValue> multiFieldMap = new HashMap<>();
        multiFieldMap.put("field1", AttributeValue.builder().s("a").build());
        multiFieldMap.put("field2", AttributeValue.builder().s("b").build());
        multiFieldMap.put("field3", AttributeValue.builder().s("c").build());

        UpdateItemRequest.Builder seed = UpdateItemRequest.builder().tableName(tableName).key(key);
        seed.updateExpression("SET MyMap = :init");
        Map<String, AttributeValue> seedVals = new HashMap<>();
        seedVals.put(":init", AttributeValue.builder().m(multiFieldMap).build());
        seed.expressionAttributeValues(seedVals);
        dynamoDbClient.updateItem(seed.build());
        phoenixDBClientV2.updateItem(seed.build());

        UpdateItemRequest.Builder uir1 = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir1.updateExpression("REMOVE MyMap.field2");
        uir1.returnValues(ReturnValue.UPDATED_OLD);
        UpdateItemResponse dynamoResult1 = dynamoDbClient.updateItem(uir1.build());
        UpdateItemResponse phoenixResult1 = phoenixDBClientV2.updateItem(uir1.build());
        Assert.assertEquals(dynamoResult1.attributes(), phoenixResult1.attributes());

        dynamoDbClient.updateItem(seed.build());
        phoenixDBClientV2.updateItem(seed.build());

        UpdateItemRequest.Builder uir2 = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir2.updateExpression("REMOVE MyMap.field2");
        uir2.returnValues(ReturnValue.UPDATED_NEW);
        UpdateItemResponse dynamoResult2 = dynamoDbClient.updateItem(uir2.build());
        UpdateItemResponse phoenixResult2 = phoenixDBClientV2.updateItem(uir2.build());
        Assert.assertEquals(dynamoResult2.attributes(), phoenixResult2.attributes());

        Map<String, AttributeValue> singleFieldMap = new HashMap<>();
        singleFieldMap.put("onlyField", AttributeValue.builder().s("x").build());
        UpdateItemRequest.Builder seedSingle =
            UpdateItemRequest.builder().tableName(tableName).key(key);
        seedSingle.updateExpression("SET MyMap = :init");
        Map<String, AttributeValue> seedSingleVals = new HashMap<>();
        seedSingleVals.put(":init", AttributeValue.builder().m(singleFieldMap).build());
        seedSingle.expressionAttributeValues(seedSingleVals);
        dynamoDbClient.updateItem(seedSingle.build());
        phoenixDBClientV2.updateItem(seedSingle.build());

        UpdateItemRequest.Builder uir3 = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir3.updateExpression("REMOVE MyMap.onlyField");
        uir3.returnValues(ReturnValue.UPDATED_NEW);
        UpdateItemResponse dynamoResult3 = dynamoDbClient.updateItem(uir3.build());
        UpdateItemResponse phoenixResult3 = phoenixDBClientV2.updateItem(uir3.build());
        Assert.assertEquals(dynamoResult3.attributes(), phoenixResult3.attributes());

        dynamoDbClient.updateItem(seed.build());
        phoenixDBClientV2.updateItem(seed.build());

        UpdateItemRequest.Builder uir4 = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir4.updateExpression("REMOVE MyMap.field2");
        uir4.returnValues(ReturnValue.ALL_OLD);
        UpdateItemResponse dynamoResult4 = dynamoDbClient.updateItem(uir4.build());
        UpdateItemResponse phoenixResult4 = phoenixDBClientV2.updateItem(uir4.build());
        Assert.assertEquals(dynamoResult4.attributes(), phoenixResult4.attributes());

        dynamoDbClient.updateItem(seed.build());
        phoenixDBClientV2.updateItem(seed.build());

        UpdateItemRequest.Builder uir5 = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir5.updateExpression("REMOVE MyMap.field2");
        uir5.returnValues(ReturnValue.ALL_NEW);
        UpdateItemResponse dynamoResult5 = dynamoDbClient.updateItem(uir5.build());
        UpdateItemResponse phoenixResult5 = phoenixDBClientV2.updateItem(uir5.build());
        Assert.assertEquals(dynamoResult5.attributes(), phoenixResult5.attributes());

        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testUpdatedOldNewRemoveDeepMixed() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);
        Map<String, AttributeValue> key = getKey();

        // Seed MyHolder.subList = [
        //   { leaf: { targetAttr: "X0", siblingAttr: "Z0" } },
        //   { leaf: { targetAttr: "X1", siblingAttr: "Z1" } }
        // ]
        // Sibling attr means REMOVE MyHolder.subList[1].leaf.targetAttr does not empty
        // the parent `leaf` map — keeps the leaf-existence vs parent-existence distinction sharp.
        Map<String, AttributeValue> leaf0 = new HashMap<>();
        leaf0.put("targetAttr", AttributeValue.builder().s("X0").build());
        leaf0.put("siblingAttr", AttributeValue.builder().s("Z0").build());

        Map<String, AttributeValue> leaf1 = new HashMap<>();
        leaf1.put("targetAttr", AttributeValue.builder().s("X1").build());
        leaf1.put("siblingAttr", AttributeValue.builder().s("Z1").build());

        Map<String, AttributeValue> elem0 = new HashMap<>();
        elem0.put("leaf", AttributeValue.builder().m(leaf0).build());
        Map<String, AttributeValue> elem1 = new HashMap<>();
        elem1.put("leaf", AttributeValue.builder().m(leaf1).build());

        Map<String, AttributeValue> holder = new HashMap<>();
        holder.put("subList", AttributeValue.builder().l(
            AttributeValue.builder().m(elem0).build(),
            AttributeValue.builder().m(elem1).build()).build());

        UpdateItemRequest.Builder seed = UpdateItemRequest.builder().tableName(tableName).key(key);
        seed.updateExpression("SET MyHolder = :init");
        Map<String, AttributeValue> seedVals = new HashMap<>();
        seedVals.put(":init", AttributeValue.builder().m(holder).build());
        seed.expressionAttributeValues(seedVals);
        dynamoDbClient.updateItem(seed.build());
        phoenixDBClientV2.updateItem(seed.build());

        // 5-segment REMOVE: MyHolder . subList [1] . leaf . targetAttr
        final String removeExpr = "REMOVE MyHolder.subList[1].leaf.targetAttr";

        UpdateItemRequest.Builder uir1 = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir1.updateExpression(removeExpr);
        uir1.returnValues(ReturnValue.UPDATED_OLD);
        UpdateItemResponse dynamoResult1 = dynamoDbClient.updateItem(uir1.build());
        UpdateItemResponse phoenixResult1 = phoenixDBClientV2.updateItem(uir1.build());
        Assert.assertEquals(dynamoResult1.attributes(), phoenixResult1.attributes());

        dynamoDbClient.updateItem(seed.build());
        phoenixDBClientV2.updateItem(seed.build());

        UpdateItemRequest.Builder uir2 = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir2.updateExpression(removeExpr);
        uir2.returnValues(ReturnValue.UPDATED_NEW);
        UpdateItemResponse dynamoResult2 = dynamoDbClient.updateItem(uir2.build());
        UpdateItemResponse phoenixResult2 = phoenixDBClientV2.updateItem(uir2.build());
        Assert.assertEquals(dynamoResult2.attributes(), phoenixResult2.attributes());

        dynamoDbClient.updateItem(seed.build());
        phoenixDBClientV2.updateItem(seed.build());

        UpdateItemRequest.Builder uir3 = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir3.updateExpression(removeExpr);
        uir3.returnValues(ReturnValue.ALL_OLD);
        UpdateItemResponse dynamoResult3 = dynamoDbClient.updateItem(uir3.build());
        UpdateItemResponse phoenixResult3 = phoenixDBClientV2.updateItem(uir3.build());
        Assert.assertEquals(dynamoResult3.attributes(), phoenixResult3.attributes());

        dynamoDbClient.updateItem(seed.build());
        phoenixDBClientV2.updateItem(seed.build());

        UpdateItemRequest.Builder uir4 = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir4.updateExpression(removeExpr);
        uir4.returnValues(ReturnValue.ALL_NEW);
        UpdateItemResponse dynamoResult4 = dynamoDbClient.updateItem(uir4.build());
        UpdateItemResponse phoenixResult4 = phoenixDBClientV2.updateItem(uir4.build());
        Assert.assertEquals(dynamoResult4.attributes(), phoenixResult4.attributes());

        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testUpdatedNewEmptyUpdateRejected() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);
        UpdateItemRequest req = UpdateItemRequest.builder().tableName(tableName).key(getKey())
            .returnValues(ReturnValue.UPDATED_NEW).build();
        int dynamoStatusCode = -1;
        int phoenixStatusCode = -1;
        try {
            dynamoDbClient.updateItem(req);
            Assert.fail("Expected DDB to reject UPDATED_NEW with no update content");
        } catch (DynamoDbException e) {
            dynamoStatusCode = e.statusCode();
        }
        try {
            phoenixDBClientV2.updateItem(req);
            Assert.fail("Expected Phoenix to reject UPDATED_NEW with no update content");
        } catch (DynamoDbException e) {
            phoenixStatusCode = e.statusCode();
        }
        Assert.assertEquals("DDB and Phoenix should agree on status code", dynamoStatusCode,
            phoenixStatusCode);
        Assert.assertEquals("Should be 5xx", 500, phoenixStatusCode);
    }
}
