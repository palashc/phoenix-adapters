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

import org.bson.BsonDocument;
import org.bson.RawBsonDocument;
import org.junit.Assert;
import org.junit.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.ddb.bson.DdbAttributesToBsonDocument;
import org.apache.phoenix.ddb.bson.UpdateExpressionDdbToBson;

public class UpdateExpressionConversionTest {

  @Test(timeout = 120000)
  public void test1() {

    String ddbUpdateExp = "SET Title = :newTitle, aCol = if_not_exists (aCol, :aCol) , "
        + "Id = :newId , NestedMap1.ColorList = :ColorList , "
        + "Id1 = :Id1 , NestedMap1.NList1[0] = :NList1_0 , "
        + "NestedList1[2][1].ISBN = :NestedList1_ISBN , "
        + "NestedMap1.NestedMap2.NewID = :newId , "
        + "NestedMap1.NestedMap2.NList[2] = :NList003 , "
        + "NestedMap1.NestedMap2.NList[0] = :NList001 ADD AddedId :attr5_0 , "
        + "NestedMap1.AddedId :attr5_0, NestedMap1.NestedMap2.Id :newIdNeg , "
        + "NestedList12[2][0] :NestedList12_00 , NestedList12[2][1] :NestedList12_01 ,"
        + "  Pictures  :AddedPics "
        + "REMOVE IdS, Id2, NestedMap1.Title , "
        + "NestedMap1.NestedMap2.InPublication , NestedList1[2][1].TitleSet1 "
        + "DELETE PictureBinarySet :PictureBinarySet01 , NestedMap1.NSet1 :NSet01 ,"
        + "  NestedList1[2][1].TitleSet2 :NestedList1TitleSet01";

    String expectedBsonUpdateExpression = "{\n"
        + "  \"$SET\": {\n"
        + "    \"Title\": \"Cycle_1234_new\",\n"
        + "    \"aCol\": {\n"
        + "       \"$IF_NOT_EXISTS\": {\n"
        + "         \"aCol\": {\n"
        + "           \"$set\": [\n"
        + "               1,\n"
        + "               2,\n"
        + "               3\n"
        + "             ]\n"
        + "           }\n"
        + "         }\n"
        + "    },\n"
        + "    \"Id\": \"12345\",\n"
        + "    \"NestedMap1.ColorList\": [\n"
        + "      \"Black\",\n"
        + "      {\n"
        + "        \"$binary\": {\n"
        + "          \"base64\": \"V2hpdGU=\",\n"
        + "          \"subType\": \"00\"\n"
        + "        }\n"
        + "      },\n"
        + "      \"Silver\"\n"
        + "    ],\n"
        + "    \"Id1\": {\n"
        + "      \"$binary\": {\n"
        + "        \"base64\": \"SURfMTAx\",\n"
        + "        \"subType\": \"00\"\n"
        + "      }\n"
        + "    },\n"
        + "    \"NestedMap1.NList1[0]\": {\n"
        + "      \"$set\": [\n"
        + "        \"Updated_set_01\",\n"
        + "        \"Updated_set_02\"\n"
        + "      ]\n"
        + "    },\n"
        + "    \"NestedList1[2][1].ISBN\": \"111-1111111122\",\n"
        + "    \"NestedMap1.NestedMap2.NewID\": \"12345\",\n"
        + "    \"NestedMap1.NestedMap2.NList[2]\": null,\n"
        + "    \"NestedMap1.NestedMap2.NList[0]\": 12.22\n"
        + "  },\n"
        + "  \"$UNSET\": {\n"
        + "    \"IdS\": null,\n"
        + "    \"Id2\": null,\n"
        + "    \"NestedMap1.Title\": null,\n"
        + "    \"NestedMap1.NestedMap2.InPublication\": null,\n"
        + "    \"NestedList1[2][1].TitleSet1\": null\n"
        + "  },\n"
        + "  \"$ADD\": {\n"
        + "    \"AddedId\": 10,\n"
        + "    \"NestedMap1.AddedId\": 10,\n"
        + "    \"NestedMap1.NestedMap2.Id\": -12345,\n"
        + "    \"NestedList12[2][0]\": {\n"
        + "      \"$set\": [\n"
        + "        \"xyz01234\",\n"
        + "        \"abc01234\"\n"
        + "      ]\n"
        + "    },\n"
        + "    \"NestedList12[2][1]\": {\n"
        + "      \"$set\": [\n"
        + "        {\n"
        + "          \"$binary\": {\n"
        + "            \"base64\": \"dmFsMDM=\",\n"
        + "            \"subType\": \"00\"\n"
        + "          }\n"
        + "        },\n"
        + "        {\n"
        + "          \"$binary\": {\n"
        + "            \"base64\": \"dmFsMDQ=\",\n"
        + "            \"subType\": \"00\"\n"
        + "          }\n"
        + "        }\n"
        + "      ]\n"
        + "    },\n"
        + "    \"Pictures\": {\n"
        + "      \"$set\": [\n"
        + "        \"1235@_rear.jpg\",\n"
        + "        \"xyz5@_rear.jpg\"\n"
        + "      ]\n"
        + "    }\n"
        + "  },\n"
        + "  \"$DELETE_FROM_SET\": {\n"
        + "    \"PictureBinarySet\": {\n"
        + "      \"$set\": [\n"
        + "        {\n"
        + "          \"$binary\": {\n"
        + "            \"base64\": \"MTIzX3JlYXIuanBn\",\n"
        + "            \"subType\": \"00\"\n"
        + "          }\n"
        + "        },\n"
        + "        {\n"
        + "          \"$binary\": {\n"
        + "            \"base64\": \"eHl6X2Zyb250LmpwZw==\",\n"
        + "            \"subType\": \"00\"\n"
        + "          }\n"
        + "        },\n"
        + "        {\n"
        + "          \"$binary\": {\n"
        + "            \"base64\": \"eHl6X2Zyb250LmpwZ19ubw==\",\n"
        + "            \"subType\": \"00\"\n"
        + "          }\n"
        + "        }\n"
        + "      ]\n"
        + "    },\n"
        + "    \"NestedMap1.NSet1\": {\n"
        + "      \"$set\": [\n"
        + "        -6830.5555,\n"
        + "        -48695\n"
        + "      ]\n"
        + "    },\n"
        + "    \"NestedList1[2][1].TitleSet2\": {\n"
        + "      \"$set\": [\n"
        + "        \"Book 1010 Title\",\n"
        + "        \"Book 1011 Title\"\n"
        + "      ]\n"
        + "    }\n"
        + "  }\n"
        + "}";


    Assert.assertEquals(RawBsonDocument.parse(expectedBsonUpdateExpression),
        UpdateExpressionDdbToBson.getBsonDocumentForUpdateExpression(ddbUpdateExp,
            getComparisonValuesMap()));
  }

  @Test
  public void test2() {
    String ddbUpdateExp = "SET bCol = if_not_exists(bCol, :aCol)";
    String expectedBsonUpdateExpression = "{\n" +
            "  \"$SET\": {\n" +
            "    \"bCol\": {\n" +
            "      \"$IF_NOT_EXISTS\": {\n" +
            "        \"bCol\": {\n" +
            "          \"$set\": [\n" +
            "            1,\n" +
            "            2,\n" +
            "            3\n" +
            "          ]\n" +
            "        }\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}";
    Assert.assertEquals(RawBsonDocument.parse(expectedBsonUpdateExpression),
            UpdateExpressionDdbToBson.getBsonDocumentForUpdateExpression(ddbUpdateExp,
                    getComparisonValuesMap()));
  }

  @Test
  public void test3() {
    String ddbUpdateExp = "SET bCol = if_not_exists(bCol, :aCol)," +
            "aCol=if_not_exists( aCol,:aCol ), cCol =if_not_exists (cCol, :aCol)";
    String expectedBsonUpdateExpression = "{\n" +
            "  \"$SET\": {\n" +
            "    \"bCol\": {\n" +
            "      \"$IF_NOT_EXISTS\": {\n" +
            "        \"bCol\": {\n" +
            "          \"$set\": [\n" +
            "            1,\n" +
            "            2,\n" +
            "            3\n" +
            "          ]\n" +
            "        }\n" +
            "      }\n" +
            "    },\n" +
            "    \"aCol\": {\n" +
            "      \"$IF_NOT_EXISTS\": {\n" +
            "        \"aCol\": {\n" +
            "          \"$set\": [\n" +
            "            1,\n" +
            "            2,\n" +
            "            3\n" +
            "          ]\n" +
            "        }\n" +
            "      }\n" +
            "    },\n" +
            "    \"cCol\": {\n" +
            "      \"$IF_NOT_EXISTS\": {\n" +
            "        \"cCol\": {\n" +
            "          \"$set\": [\n" +
            "            1,\n" +
            "            2,\n" +
            "            3\n" +
            "          ]\n" +
            "        }\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}";
    Assert.assertEquals(RawBsonDocument.parse(expectedBsonUpdateExpression),
            UpdateExpressionDdbToBson.getBsonDocumentForUpdateExpression(ddbUpdateExp,
                    getComparisonValuesMap()));
  }

  @Test
  public void testListAppendPathAndLiteral() {
    String ddbUpdateExp = "SET myList = list_append(myList, :listVal)";
    String expected = "{\n" +
            "  \"$SET\": {\n" +
            "    \"myList\": {\n" +
            "      \"$LIST_APPEND\": [\n" +
            "        \"myList\",\n" +
            "        [\"c\", \"d\"]\n" +
            "      ]\n" +
            "    }\n" +
            "  }\n" +
            "}";
    Assert.assertEquals(RawBsonDocument.parse(expected),
            UpdateExpressionDdbToBson.getBsonDocumentForUpdateExpression(ddbUpdateExp,
                    getListAppendComparisonValuesMap()));
  }

  @Test
  public void testListAppendLiteralAndPath() {
    String ddbUpdateExp = "SET myList = list_append(:listVal, myList)";
    String expected = "{\n" +
            "  \"$SET\": {\n" +
            "    \"myList\": {\n" +
            "      \"$LIST_APPEND\": [\n" +
            "        [\"c\", \"d\"],\n" +
            "        \"myList\"\n" +
            "      ]\n" +
            "    }\n" +
            "  }\n" +
            "}";
    Assert.assertEquals(RawBsonDocument.parse(expected),
            UpdateExpressionDdbToBson.getBsonDocumentForUpdateExpression(ddbUpdateExp,
                    getListAppendComparisonValuesMap()));
  }

  @Test
  public void testListAppendBothLiterals() {
    String ddbUpdateExp = "SET myList = list_append(:listVal, :listVal2)";
    String expected = "{\n" +
            "  \"$SET\": {\n" +
            "    \"myList\": {\n" +
            "      \"$LIST_APPEND\": [\n" +
            "        [\"c\", \"d\"],\n" +
            "        [\"e\", \"f\"]\n" +
            "      ]\n" +
            "    }\n" +
            "  }\n" +
            "}";
    Assert.assertEquals(RawBsonDocument.parse(expected),
            UpdateExpressionDdbToBson.getBsonDocumentForUpdateExpression(ddbUpdateExp,
                    getListAppendComparisonValuesMap()));
  }

  /**
   * Canonical create-or-append: if the target list does not exist, fall back to an empty
   * list as the first operand and then append the new literal items.
   */
  @Test
  public void testListAppendWithIfNotExists() {
    String ddbUpdateExp = "SET newQueue = list_append(if_not_exists(newQueue, :emptyList), :listVal)";
    String expected = "{\n" +
            "  \"$SET\": {\n" +
            "    \"newQueue\": {\n" +
            "      \"$LIST_APPEND\": [\n" +
            "        {\n" +
            "          \"$IF_NOT_EXISTS\": {\n" +
            "            \"newQueue\": []\n" +
            "          }\n" +
            "        },\n" +
            "        [\"c\", \"d\"]\n" +
            "      ]\n" +
            "    }\n" +
            "  }\n" +
            "}";
    Assert.assertEquals(RawBsonDocument.parse(expected),
            UpdateExpressionDdbToBson.getBsonDocumentForUpdateExpression(ddbUpdateExp,
                    getListAppendComparisonValuesMap()));
  }

  /**
   * list_append combined with arithmetic and a remove in the same expression.
   */
  @Test
  public void testListAppendMixedWithArithmeticAndRemove() {
    String ddbUpdateExp =
            "SET myList = list_append(myList, :listVal), counter = counter + :one "
                    + "REMOVE oldField";
    String expected = "{\n" +
            "  \"$SET\": {\n" +
            "    \"myList\": {\n" +
            "      \"$LIST_APPEND\": [\n" +
            "        \"myList\",\n" +
            "        [\"c\", \"d\"]\n" +
            "      ]\n" +
            "    },\n" +
            "    \"counter\": {\n" +
            "      \"$ADD\": [\n" +
            "        \"counter\",\n" +
            "        1\n" +
            "      ]\n" +
            "    }\n" +
            "  },\n" +
            "  \"$UNSET\": {\n" +
            "    \"oldField\": null\n" +
            "  }\n" +
            "}";
    Assert.assertEquals(RawBsonDocument.parse(expected),
            UpdateExpressionDdbToBson.getBsonDocumentForUpdateExpression(ddbUpdateExp,
                    getListAppendComparisonValuesMap()));
  }

  /**
   * Nested document path on the LHS and as the path operand.
   */
  @Test
  public void testListAppendNestedPath() {
    String ddbUpdateExp = "SET nested.queue = list_append(nested.queue, :listVal)";
    String expected = "{\n" +
            "  \"$SET\": {\n" +
            "    \"nested.queue\": {\n" +
            "      \"$LIST_APPEND\": [\n" +
            "        \"nested.queue\",\n" +
            "        [\"c\", \"d\"]\n" +
            "      ]\n" +
            "    }\n" +
            "  }\n" +
            "}";
    Assert.assertEquals(RawBsonDocument.parse(expected),
            UpdateExpressionDdbToBson.getBsonDocumentForUpdateExpression(ddbUpdateExp,
                    getListAppendComparisonValuesMap()));
  }

  /**
   * if_not_exists as the SECOND operand (literal first). This prepends the new items
   * before the existing-or-fallback list.
   */
  @Test
  public void testListAppendLiteralAndIfNotExists() {
    String ddbUpdateExp = "SET newQueue = list_append(:listVal, if_not_exists(newQueue, :emptyList))";
    String expected = "{\n" +
            "  \"$SET\": {\n" +
            "    \"newQueue\": {\n" +
            "      \"$LIST_APPEND\": [\n" +
            "        [\"c\", \"d\"],\n" +
            "        {\n" +
            "          \"$IF_NOT_EXISTS\": {\n" +
            "            \"newQueue\": []\n" +
            "          }\n" +
            "        }\n" +
            "      ]\n" +
            "    }\n" +
            "  }\n" +
            "}";
    Assert.assertEquals(RawBsonDocument.parse(expected),
            UpdateExpressionDdbToBson.getBsonDocumentForUpdateExpression(ddbUpdateExp,
                    getListAppendComparisonValuesMap()));
  }

  /**
   * Create-or-append a list while atomically incrementing a counter in the same SET clause.
   */
  @Test
  public void testListAppendWithIfNotExistsAndCounterIncrement() {
    String ddbUpdateExp =
            "SET events = list_append(if_not_exists(events, :empty_list), :new_evts), "
                    + "updateCounter = updateCounter + :one";
    String expected = "{\n" +
            "  \"$SET\": {\n" +
            "    \"events\": {\n" +
            "      \"$LIST_APPEND\": [\n" +
            "        {\n" +
            "          \"$IF_NOT_EXISTS\": {\n" +
            "            \"events\": []\n" +
            "          }\n" +
            "        },\n" +
            "        [\"ev1\", \"ev2\"]\n" +
            "      ]\n" +
            "    },\n" +
            "    \"updateCounter\": {\n" +
            "      \"$ADD\": [\n" +
            "        \"updateCounter\",\n" +
            "        1\n" +
            "      ]\n" +
            "    }\n" +
            "  }\n" +
            "}";
    Map<String, AttributeValue> attributeMap = new HashMap<>();
    attributeMap.put(":empty_list",
            AttributeValue.builder().l(Collections.emptyList()).build());
    attributeMap.put(":new_evts", AttributeValue.builder().l(
            AttributeValue.builder().s("ev1").build(),
            AttributeValue.builder().s("ev2").build()).build());
    attributeMap.put(":one", AttributeValue.builder().n("1").build());
    BsonDocument values = DdbAttributesToBsonDocument.getRawBsonDocument(attributeMap);

    Assert.assertEquals(RawBsonDocument.parse(expected),
            UpdateExpressionDdbToBson.getBsonDocumentForUpdateExpression(ddbUpdateExp, values));
  }

  /**
   * Both operands of list_append are if_not_exists. Regression test for the
   * paren-counting splitter: there are top-level commas at depth 1 followed by
   * another open-paren on each side, which the old regex-based splitter could not
   * handle.
   */
  @Test
  public void testListAppendBothOperandsAreIfNotExists() {
    String ddbUpdateExp =
            "SET merged = list_append(if_not_exists(left, :emptyList), if_not_exists(right, :emptyList))";
    String expected = "{\n" +
            "  \"$SET\": {\n" +
            "    \"merged\": {\n" +
            "      \"$LIST_APPEND\": [\n" +
            "        { \"$IF_NOT_EXISTS\": { \"left\":  [] } },\n" +
            "        { \"$IF_NOT_EXISTS\": { \"right\": [] } }\n" +
            "      ]\n" +
            "    }\n" +
            "  }\n" +
            "}";
    Assert.assertEquals(RawBsonDocument.parse(expected),
            UpdateExpressionDdbToBson.getBsonDocumentForUpdateExpression(ddbUpdateExp,
                    getListAppendComparisonValuesMap()));
  }

  /**
   * Canonical counter-init pattern: increment a counter that may not exist yet, using
   * if_not_exists as one of the arithmetic operands. Common DDB usage but previously
   * only covered by IT tests.
   */
  @Test
  public void testArithmeticWithIfNotExistsCounterIncrement() {
    String ddbUpdateExp = "SET counter = if_not_exists(counter, :zero) + :one";
    String expected = "{\n" +
            "  \"$SET\": {\n" +
            "    \"counter\": {\n" +
            "      \"$ADD\": [\n" +
            "        { \"$IF_NOT_EXISTS\": { \"counter\": 0 } },\n" +
            "        1\n" +
            "      ]\n" +
            "    }\n" +
            "  }\n" +
            "}";
    Map<String, AttributeValue> attributeMap = new HashMap<>();
    attributeMap.put(":zero", AttributeValue.builder().n("0").build());
    attributeMap.put(":one", AttributeValue.builder().n("1").build());
    BsonDocument values = DdbAttributesToBsonDocument.getRawBsonDocument(attributeMap);
    Assert.assertEquals(RawBsonDocument.parse(expected),
            UpdateExpressionDdbToBson.getBsonDocumentForUpdateExpression(ddbUpdateExp, values));
  }

  /**
   * Arithmetic with both operands being attribute paths (no placeholders, no
   * if_not_exists).
   */
  @Test
  public void testArithmeticPathPlusPath() {
    String ddbUpdateExp = "SET total = subtotal + tax";
    String expected = "{\n" +
            "  \"$SET\": {\n" +
            "    \"total\": {\n" +
            "      \"$ADD\": [\n" +
            "        \"subtotal\",\n" +
            "        \"tax\"\n" +
            "      ]\n" +
            "    }\n" +
            "  }\n" +
            "}";
    Assert.assertEquals(RawBsonDocument.parse(expected),
            UpdateExpressionDdbToBson.getBsonDocumentForUpdateExpression(ddbUpdateExp,
                    new BsonDocument()));
  }

  /**
   * Subtract with placeholder first and attribute path second
   * (e.g. {@code SET remaining = :limit - used}).
   */
  @Test
  public void testArithmeticLiteralMinusPath() {
    String ddbUpdateExp = "SET remaining = :limit - used";
    String expected = "{\n" +
            "  \"$SET\": {\n" +
            "    \"remaining\": {\n" +
            "      \"$SUBTRACT\": [\n" +
            "        100,\n" +
            "        \"used\"\n" +
            "      ]\n" +
            "    }\n" +
            "  }\n" +
            "}";
    Map<String, AttributeValue> attributeMap = new HashMap<>();
    attributeMap.put(":limit", AttributeValue.builder().n("100").build());
    BsonDocument values = DdbAttributesToBsonDocument.getRawBsonDocument(attributeMap);
    Assert.assertEquals(RawBsonDocument.parse(expected),
            UpdateExpressionDdbToBson.getBsonDocumentForUpdateExpression(ddbUpdateExp, values));
  }

  /**
   * Multiple SET clauses interleaving every supported function/operator shape:
   * plain placeholder, if_not_exists, arithmetic with if_not_exists, list_append,
   * and a bare nested-path assignment.
   */
  @Test
  public void testSetEveryShapeMixed() {
    String ddbUpdateExp = "SET title = :newTitle, "
            + "counter = if_not_exists(counter, :zero) + :one, "
            + "total = price - :discount, "
            + "events = list_append(if_not_exists(events, :emptyList), :newEvts), "
            + "nested.path = :nestedVal";
    String expected = "{\n" +
            "  \"$SET\": {\n" +
            "    \"title\": \"hello\",\n" +
            "    \"counter\": {\n" +
            "      \"$ADD\": [\n" +
            "        { \"$IF_NOT_EXISTS\": { \"counter\": 0 } },\n" +
            "        1\n" +
            "      ]\n" +
            "    },\n" +
            "    \"total\": {\n" +
            "      \"$SUBTRACT\": [\n" +
            "        \"price\",\n" +
            "        5\n" +
            "      ]\n" +
            "    },\n" +
            "    \"events\": {\n" +
            "      \"$LIST_APPEND\": [\n" +
            "        { \"$IF_NOT_EXISTS\": { \"events\": [] } },\n" +
            "        [\"e1\", \"e2\"]\n" +
            "      ]\n" +
            "    },\n" +
            "    \"nested.path\": \"deep\"\n" +
            "  }\n" +
            "}";
    Map<String, AttributeValue> attributeMap = new HashMap<>();
    attributeMap.put(":newTitle", AttributeValue.builder().s("hello").build());
    attributeMap.put(":zero", AttributeValue.builder().n("0").build());
    attributeMap.put(":one", AttributeValue.builder().n("1").build());
    attributeMap.put(":discount", AttributeValue.builder().n("5").build());
    attributeMap.put(":emptyList",
            AttributeValue.builder().l(Collections.emptyList()).build());
    attributeMap.put(":newEvts", AttributeValue.builder().l(
            AttributeValue.builder().s("e1").build(),
            AttributeValue.builder().s("e2").build()).build());
    attributeMap.put(":nestedVal", AttributeValue.builder().s("deep").build());
    BsonDocument values = DdbAttributesToBsonDocument.getRawBsonDocument(attributeMap);
    Assert.assertEquals(RawBsonDocument.parse(expected),
            UpdateExpressionDdbToBson.getBsonDocumentForUpdateExpression(ddbUpdateExp, values));
  }

  @Test
  public void testListAppendWrongArityOne() {
    String ddbUpdateExp = "SET myList = list_append(:listVal)";
    try {
      UpdateExpressionDdbToBson.getBsonDocumentForUpdateExpression(ddbUpdateExp,
              getListAppendComparisonValuesMap());
      Assert.fail("Expected RuntimeException for wrong arity (1 operand)");
    } catch (RuntimeException e) {
      Assert.assertTrue("Unexpected message: " + e.getMessage(),
              e.getMessage().contains("list_append requires exactly 2 operands"));
    }
  }

  @Test
  public void testListAppendWrongArityThree() {
    String ddbUpdateExp = "SET myList = list_append(:listVal, :listVal2, myList)";
    try {
      UpdateExpressionDdbToBson.getBsonDocumentForUpdateExpression(ddbUpdateExp,
              getListAppendComparisonValuesMap());
      Assert.fail("Expected RuntimeException for wrong arity (3 operands)");
    } catch (RuntimeException e) {
      Assert.assertTrue("Unexpected message: " + e.getMessage(),
              e.getMessage().contains("list_append requires exactly 2 operands"));
    }
  }

  @Test
  public void testListAppendMissingPlaceholder() {
    String ddbUpdateExp = "SET myList = list_append(myList, :missing)";
    try {
      UpdateExpressionDdbToBson.getBsonDocumentForUpdateExpression(ddbUpdateExp,
              getListAppendComparisonValuesMap());
      Assert.fail("Expected RuntimeException for missing placeholder");
    } catch (RuntimeException e) {
      Assert.assertTrue("Unexpected message: " + e.getMessage(),
              e.getMessage().contains(":missing"));
    }
  }

  @Test
  public void testListAppendNonArrayPlaceholder() {
    String ddbUpdateExp = "SET myList = list_append(myList, :notList)";
    try {
      UpdateExpressionDdbToBson.getBsonDocumentForUpdateExpression(ddbUpdateExp,
              getListAppendComparisonValuesMap());
      Assert.fail("Expected RuntimeException for non-list placeholder");
    } catch (RuntimeException e) {
      Assert.assertTrue("Unexpected message: " + e.getMessage(),
              e.getMessage().contains("must resolve to a List type"));
    }
  }

  private static BsonDocument getListAppendComparisonValuesMap() {
    Map<String, AttributeValue> attributeMap = new HashMap<>();
    attributeMap.put(":listVal", AttributeValue.builder().l(
            AttributeValue.builder().s("c").build(),
            AttributeValue.builder().s("d").build()).build());
    attributeMap.put(":listVal2", AttributeValue.builder().l(
            AttributeValue.builder().s("e").build(),
            AttributeValue.builder().s("f").build()).build());
    attributeMap.put(":emptyList",
            AttributeValue.builder().l(Collections.emptyList()).build());
    attributeMap.put(":one", AttributeValue.builder().n("1").build());
    attributeMap.put(":notList", AttributeValue.builder().s("scalar").build());
    return DdbAttributesToBsonDocument.getRawBsonDocument(attributeMap);
  }

  private static BsonDocument getComparisonValuesMap() {
    Map<String, AttributeValue> attributeMap = new HashMap<>();
    attributeMap.put(":aCol", AttributeValue.builder().ns("1", "2", "3").build());
    attributeMap.put(":newTitle", AttributeValue.builder().s("Cycle_1234_new").build());
    attributeMap.put(":newId", AttributeValue.builder().s("12345").build());
    attributeMap.put(":newIdNeg", AttributeValue.builder().n("-12345").build());
    attributeMap.put(":ColorList", AttributeValue.builder().l(
        AttributeValue.builder().s("Black").build(),
        AttributeValue.builder().b(SdkBytes.fromByteArray(Bytes.toBytes("White"))).build(),
        AttributeValue.builder().s("Silver").build()
    ).build());
    attributeMap.put(":Id1",
        AttributeValue.builder().b(SdkBytes.fromByteArray(Bytes.toBytes("ID_101"))).build());
    attributeMap.put(":NList001", AttributeValue.builder().n("12.22").build());
    attributeMap.put(":NList003", AttributeValue.builder().nul(true).build());
    attributeMap.put(":NList004", AttributeValue.builder().bool(true).build());
    attributeMap.put(":attr5_0", AttributeValue.builder().n("10").build());
    attributeMap.put(":NList1_0", AttributeValue.builder().ss("Updated_set_01", "Updated_set_02").build());
    attributeMap.put(":NestedList1_ISBN", AttributeValue.builder().s("111-1111111122").build());
    attributeMap.put(":NestedList12_00", AttributeValue.builder().ss("xyz01234", "abc01234").build());
    attributeMap.put(":NestedList12_01", AttributeValue.builder().bs(
        SdkBytes.fromByteArray(Bytes.toBytes("val03")),
        SdkBytes.fromByteArray(Bytes.toBytes("val04"))
    ).build());
    attributeMap.put(":AddedPics", AttributeValue.builder().ss(
        "1235@_rear.jpg",
        "xyz5@_rear.jpg").build());
    attributeMap.put(":PictureBinarySet01", AttributeValue.builder().bs(
        SdkBytes.fromByteArray(Bytes.toBytes("123_rear.jpg")),
        SdkBytes.fromByteArray(Bytes.toBytes("xyz_front.jpg")),
        SdkBytes.fromByteArray(Bytes.toBytes("xyz_front.jpg_no"))
    ).build());
    attributeMap.put(":NSet01", AttributeValue.builder().ns("-6830.5555", "-48695").build());
    attributeMap.put(":NestedList1TitleSet01", AttributeValue.builder().ss("Book 1010 Title",
        "Book 1011 Title").build());
    return DdbAttributesToBsonDocument.getRawBsonDocument(attributeMap);
  }

}
