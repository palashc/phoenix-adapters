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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.phoenix.ddb.utils.CommonServiceUtils;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.junit.Assert;
import org.junit.Test;

public class CommonServiceUtilsTest {

    @Test
    public void testReplaceExpressionAttributeNames_nullMap_noPlaceholders() {
        String input = "SET name = :1";
        String result = CommonServiceUtils.replaceExpressionAttributeNames(input, null);
        Assert.assertEquals(input, result);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testReplaceExpressionAttributeNames_nullMap_withPlaceholders_throwsException() {
        String input = "SET #1 = :1";
        CommonServiceUtils.replaceExpressionAttributeNames(input, null);
    }

    @Test
    public void testReplaceExpressionAttributeNames_emptyMap_noPlaceholders() {
        String input = "SET name = :1";
        Map<String, String> exprAttrNames = new HashMap<>();
        String result = CommonServiceUtils.replaceExpressionAttributeNames(input, exprAttrNames);
        Assert.assertEquals(input, result);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testReplaceExpressionAttributeNames_emptyMap_withPlaceholders_throwsException() {
        String input = "SET #1 = :1";
        Map<String, String> exprAttrNames = new HashMap<>();
        CommonServiceUtils.replaceExpressionAttributeNames(input, exprAttrNames);
    }

    @Test
    public void testReplaceExpressionAttributeNames_nullOrEmptyMap_exceptionMessage() {
        String input = "SET #myAttr = :1";
        try {
            CommonServiceUtils.replaceExpressionAttributeNames(input, null);
            Assert.fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("#myAttr"));
            Assert.assertTrue(e.getMessage().contains("not provided"));
        }
    }

    @Test
    public void testReplaceExpressionAttributeNames_noHashInString() {
        String input = "SET name = :1";
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "attr1");
        String result = CommonServiceUtils.replaceExpressionAttributeNames(input, exprAttrNames);
        Assert.assertEquals(input, result);
    }

    @Test
    public void testReplaceExpressionAttributeNames_simpleReplacement() {
        String input = "SET #1 = :1";
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "myAttribute");
        String result = CommonServiceUtils.replaceExpressionAttributeNames(input, exprAttrNames);
        Assert.assertEquals("SET myAttribute = :1", result);
    }

    @Test
    public void testReplaceExpressionAttributeNames_multipleReplacements() {
        String input = "SET #1 = :1, #2 = :2, #3 = :3";
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "attr1");
        exprAttrNames.put("#2", "attr2");
        exprAttrNames.put("#3", "attr3");
        String result = CommonServiceUtils.replaceExpressionAttributeNames(input, exprAttrNames);
        Assert.assertEquals("SET attr1 = :1, attr2 = :2, attr3 = :3", result);
    }

    @Test
    public void testReplaceExpressionAttributeNames_substringIssue_hash1AndHash10() {
        // This is the key test case - #1 should not be replaced within #10
        String input = "SET #1 = :1, #10 = :10";
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "sk");
        exprAttrNames.put("#10", "task_counters");
        String result = CommonServiceUtils.replaceExpressionAttributeNames(input, exprAttrNames);
        Assert.assertEquals("SET sk = :1, task_counters = :10", result);
    }

    @Test
    public void testReplaceExpressionAttributeNames_substringIssue_hash1AndHash11AndHash111() {
        String input = "SET #1 = :1, #11 = :11, #111 = :111";
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "a");
        exprAttrNames.put("#11", "b");
        exprAttrNames.put("#111", "c");
        String result = CommonServiceUtils.replaceExpressionAttributeNames(input, exprAttrNames);
        Assert.assertEquals("SET a = :1, b = :11, c = :111", result);
    }

    @Test
    public void testReplaceExpressionAttributeNames_realTest() {
        String input = "SET #2 = :2, #3 = :3, #10 = :4, #4 = :5, #5 = :6 REMOVE #9, #7";
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#0", "hk");
        exprAttrNames.put("#1", "sk");
        exprAttrNames.put("#2", "worker_id");
        exprAttrNames.put("#3", "last_evaluated_key");
        exprAttrNames.put("#4", "execute_after");
        exprAttrNames.put("#5", "scheduled_execute_after");
        exprAttrNames.put("#6", "execution_number");
        exprAttrNames.put("#7", "ready_for_post_hook");
        exprAttrNames.put("#8", "ExpiresAtEpochSec");
        exprAttrNames.put("#9", "^");
        exprAttrNames.put("#10", "task_counters");
        
        String result = CommonServiceUtils.replaceExpressionAttributeNames(input, exprAttrNames);
        String expected = "SET worker_id = :2, last_evaluated_key = :3, task_counters = :4, " +
                "execute_after = :5, scheduled_execute_after = :6 REMOVE ^, ready_for_post_hook";
        Assert.assertEquals(expected, result);
    }

    @Test
    public void testReplaceExpressionAttributeNames_alphanumericNames() {
        String input = "SET #name = :1, #my_attr = :2";
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#name", "firstName");
        exprAttrNames.put("#my_attr", "lastName");
        String result = CommonServiceUtils.replaceExpressionAttributeNames(input, exprAttrNames);
        Assert.assertEquals("SET firstName = :1, lastName = :2", result);
    }

    @Test
    public void testReplaceExpressionAttributeNames_mixedNumericAndAlphanumeric() {
        String input = "SET #1 = :1, #name = :2, #10 = :3";
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "id");
        exprAttrNames.put("#name", "userName");
        exprAttrNames.put("#10", "score");
        String result = CommonServiceUtils.replaceExpressionAttributeNames(input, exprAttrNames);
        Assert.assertEquals("SET id = :1, userName = :2, score = :3", result);
    }

    @Test
    public void testReplaceExpressionAttributeNames_nestedAttributes() {
        String input = "SET #1.#2 = :1";
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "parent");
        exprAttrNames.put("#2", "child");
        String result = CommonServiceUtils.replaceExpressionAttributeNames(input, exprAttrNames);
        Assert.assertEquals("SET parent.child = :1", result);
    }

    @Test
    public void testReplaceExpressionAttributeNames_listIndex() {
        String input = "SET #1[0] = :1, #2[1].#3 = :2";
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "myList");
        exprAttrNames.put("#2", "nestedList");
        exprAttrNames.put("#3", "attr");
        String result = CommonServiceUtils.replaceExpressionAttributeNames(input, exprAttrNames);
        Assert.assertEquals("SET myList[0] = :1, nestedList[1].attr = :2", result);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testReplaceExpressionAttributeNames_unmatchedPlaceholder_throwsException() {
        String input = "SET #1 = :1, #2 = :2";
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "attr1");
        // #2 is not in the map - should throw exception
        CommonServiceUtils.replaceExpressionAttributeNames(input, exprAttrNames);
    }

    @Test
    public void testReplaceExpressionAttributeNames_unmatchedPlaceholder_exceptionMessage() {
        String input = "SET #1 = :1, #missing = :2";
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "attr1");
        try {
            CommonServiceUtils.replaceExpressionAttributeNames(input, exprAttrNames);
            Assert.fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("#missing"));
            Assert.assertTrue(e.getMessage().contains("ExpressionAttributeNames"));
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testReplaceExpressionAttributeNames_onlyPlaceholderMissing_throwsException() {
        String input = "SET #1 = :1";
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#other", "other_attr");
        // #1 is not in the map, but map is not empty so validation happens
        CommonServiceUtils.replaceExpressionAttributeNames(input, exprAttrNames);
    }

    @Test
    public void testReplaceExpressionAttributeNames_conditionExpression() {
        String input = "attribute_exists(#0) AND attribute_exists(#1) AND #4 = :0 AND #6 = :1";
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#0", "hk");
        exprAttrNames.put("#1", "sk");
        exprAttrNames.put("#4", "execute_after");
        exprAttrNames.put("#6", "execution_number");
        String result = CommonServiceUtils.replaceExpressionAttributeNames(input, exprAttrNames);
        Assert.assertEquals("attribute_exists(hk) AND attribute_exists(sk) AND execute_after = :0 AND execution_number = :1", result);
    }

    @Test
    public void testReplaceExpressionAttributeNames_underscoreInPlaceholder() {
        String input = "SET #my_attr_1 = :1, #my_attr_10 = :2";
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#my_attr_1", "first");
        exprAttrNames.put("#my_attr_10", "tenth");
        String result = CommonServiceUtils.replaceExpressionAttributeNames(input, exprAttrNames);
        Assert.assertEquals("SET first = :1, tenth = :2", result);
    }

    @Test
    public void testReplaceExpressionAttributeNames_emptyString() {
        String input = "";
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "attr1");
        String result = CommonServiceUtils.replaceExpressionAttributeNames(input, exprAttrNames);
        Assert.assertEquals("", result);
    }

    @Test
    public void testReplaceExpressionAttributeNames_adjacentPlaceholders() {
        String input = "#1#2";
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "a");
        exprAttrNames.put("#2", "b");
        String result = CommonServiceUtils.replaceExpressionAttributeNames(input, exprAttrNames);
        Assert.assertEquals("ab", result);
    }

    // ---- getTouchedPathsFromUpdateDoc ----

    @Test
    public void testGetTouchedPathsFromUpdateDoc_null() {
        Set<String> paths = CommonServiceUtils.getTouchedPathsFromUpdateDoc(null);
        Assert.assertNotNull(paths);
        Assert.assertTrue(paths.isEmpty());
    }

    @Test
    public void testGetTouchedPathsFromUpdateDoc_emptyDoc() {
        Set<String> paths = CommonServiceUtils.getTouchedPathsFromUpdateDoc(new BsonDocument());
        Assert.assertTrue(paths.isEmpty());
    }

    @Test
    public void testGetTouchedPathsFromUpdateDoc_setOnly() {
        BsonDocument updateDoc = new BsonDocument();
        BsonDocument setDoc = new BsonDocument();
        setDoc.put("name", new BsonString("Bob"));
        setDoc.put("address.city", new BsonString("Portland"));
        updateDoc.put("$SET", setDoc);

        Set<String> paths = CommonServiceUtils.getTouchedPathsFromUpdateDoc(updateDoc);
        Assert.assertEquals(2, paths.size());
        Assert.assertTrue(paths.contains("name"));
        Assert.assertTrue(paths.contains("address.city"));
    }

    @Test
    public void testGetTouchedPathsFromUpdateDoc_allClauses() {
        BsonDocument updateDoc = new BsonDocument();
        BsonDocument setDoc = new BsonDocument();
        setDoc.put("name", new BsonString("Bob"));
        updateDoc.put("$SET", setDoc);

        BsonDocument unsetDoc = new BsonDocument();
        unsetDoc.put("oldField", BsonNull.VALUE);
        unsetDoc.put("nested.legacy", BsonNull.VALUE);
        updateDoc.put("$UNSET", unsetDoc);

        BsonDocument addDoc = new BsonDocument();
        addDoc.put("counter", new BsonInt32(1));
        updateDoc.put("$ADD", addDoc);

        BsonDocument delDoc = new BsonDocument();
        delDoc.put("tags", new BsonString(":t1"));
        updateDoc.put("$DELETE_FROM_SET", delDoc);

        Set<String> paths = CommonServiceUtils.getTouchedPathsFromUpdateDoc(updateDoc);
        Assert.assertEquals(5, paths.size());
        Assert.assertTrue(paths.contains("name"));
        Assert.assertTrue(paths.contains("oldField"));
        Assert.assertTrue(paths.contains("nested.legacy"));
        Assert.assertTrue(paths.contains("counter"));
        Assert.assertTrue(paths.contains("tags"));
    }

    @Test
    public void testGetTouchedPathsFromUpdateDoc_listIndexPath() {
        BsonDocument updateDoc = new BsonDocument();
        BsonDocument setDoc = new BsonDocument();
        // Path produced by PathResolver for `SET items[0].sku = :v` is "items[0].sku".
        setDoc.put("items[0].sku", new BsonString("ABC"));
        updateDoc.put("$SET", setDoc);

        Set<String> paths = CommonServiceUtils.getTouchedPathsFromUpdateDoc(updateDoc);
        Assert.assertEquals(1, paths.size());
        Assert.assertTrue(paths.contains("items[0].sku"));
    }

    @Test
    public void testGetTouchedPathsFromUpdateDoc_unknownTopLevelKeysAreIgnored() {
        // The adapter never emits these, but extra keys outside the four supported clauses
        // must not pollute the touched-path set.
        BsonDocument updateDoc = new BsonDocument();
        BsonDocument setDoc = new BsonDocument();
        setDoc.put("name", new BsonString("Bob"));
        updateDoc.put("$SET", setDoc);
        updateDoc.put("$WHATEVER",
                new BsonDocument("ignored", new BsonString("x")));

        Set<String> paths = CommonServiceUtils.getTouchedPathsFromUpdateDoc(updateDoc);
        Assert.assertEquals(1, paths.size());
        Assert.assertTrue(paths.contains("name"));
    }
}
