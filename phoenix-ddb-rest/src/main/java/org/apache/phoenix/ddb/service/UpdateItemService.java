package org.apache.phoenix.ddb.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.phoenix.ddb.service.exceptions.ValidationException;
import org.apache.phoenix.expression.util.bson.BsonUpdateInvalidArgumentException;
import org.apache.phoenix.expression.util.bson.UpdateExpressionUtils;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.phoenix.ddb.bson.MapToBsonDocument;
import org.apache.phoenix.ddb.ConnectionUtil;
import org.apache.phoenix.ddb.service.exceptions.PhoenixServiceException;
import org.apache.phoenix.ddb.service.utils.DMLUtils;
import org.apache.phoenix.ddb.service.utils.ValidationUtil;
import org.apache.phoenix.ddb.utils.ApiMetadata;
import org.apache.phoenix.ddb.utils.PhoenixUtils;
import org.apache.phoenix.ddb.rest.metrics.ApiOperation;
import org.apache.phoenix.ddb.utils.CommonServiceUtils;
import org.apache.phoenix.schema.PColumn;

public class UpdateItemService {

    private static final Logger LOGGER = LoggerFactory.getLogger(UpdateItemService.class);

    private static final String UPDATE_WITH_HASH_KEY =
            "UPSERT INTO %s VALUES (?,?) " + " ON DUPLICATE KEY UPDATE\n"
                    + " COL = BSON_UPDATE_EXPRESSION(COL,?)";

    private static final String UPDATE_WITH_HASH_SORT_KEY =
            "UPSERT INTO %s VALUES (?,?,?) " + " ON DUPLICATE KEY UPDATE\n"
                    + " COL = BSON_UPDATE_EXPRESSION(COL,?)";

    private static final String UPDATE_ONLY_WITH_HASH_KEY =
            "UPSERT INTO %s VALUES (?) " + " ON DUPLICATE KEY UPDATE_ONLY\n"
                    + " COL = BSON_UPDATE_EXPRESSION(COL,?)";

    private static final String UPDATE_ONLY_WITH_HASH_SORT_KEY =
            "UPSERT INTO %s VALUES (?,?) " + " ON DUPLICATE KEY UPDATE_ONLY\n"
                    + " COL = BSON_UPDATE_EXPRESSION(COL,?)";

    private static final String CONDITIONAL_UPDATE_WITH_HASH_KEY =
            "UPSERT INTO %s VALUES (?,?) " + " ON DUPLICATE KEY UPDATE\n"
                    + " COL = CASE WHEN BSON_CONDITION_EXPRESSION(COL,?) "
                    + " THEN BSON_UPDATE_EXPRESSION(COL,?) \n" + " ELSE COL END";

    private static final String CONDITIONAL_UPDATE_WITH_HASH_SORT_KEY =
            "UPSERT INTO %s VALUES (?,?,?) " + " ON DUPLICATE KEY UPDATE\n"
                    + " COL = CASE WHEN BSON_CONDITION_EXPRESSION(COL,?) "
                    + " THEN BSON_UPDATE_EXPRESSION(COL,?) \n" + " ELSE COL END";

    private static final String CONDITIONAL_UPDATE_ONLY_WITH_HASH_KEY =
            "UPSERT INTO %s VALUES (?) " + " ON DUPLICATE KEY UPDATE_ONLY\n"
                    + " COL = CASE WHEN BSON_CONDITION_EXPRESSION(COL,?) "
                    + " THEN BSON_UPDATE_EXPRESSION(COL,?) \n" + " ELSE COL END";

    private static final String CONDITIONAL_UPDATE_ONLY_WITH_HASH_SORT_KEY =
            "UPSERT INTO %s VALUES (?,?) " + " ON DUPLICATE KEY UPDATE_ONLY\n"
                    + " COL = CASE WHEN BSON_CONDITION_EXPRESSION(COL,?) "
                    + " THEN BSON_UPDATE_EXPRESSION(COL,?) \n" + " ELSE COL END";

    public static Map<String, Object> updateItem(Map<String, Object> request,
            String connectionUrl) {
        ValidationUtil.validateUpdateItemRequest(request);
        try (Connection connection = ConnectionUtil.getConnection(connectionUrl)) {
            connection.setAutoCommit(true);
            // get PTable and PK PColumns
            List<PColumn> pkCols = PhoenixUtils.getPKColumns(connection, (String)request.get(ApiMetadata.TABLE_NAME));
            //create statement based on PKs and conditional expression
            StatementInfo statementInfo = getPreparedStatement(connection, request, pkCols);
            setValuesOnPreparedStatement(statementInfo, pkCols, request);

            //execute, auto commit is on
            LOGGER.debug("Upsert Query for UpdateItem: {}", statementInfo.stmt);
            // Determine if any condition expression is present
            boolean hasCondExp = (request.get(ApiMetadata.CONDITION_EXPRESSION) != null) || (
                    request.get(ApiMetadata.EXPECTED) != null);

            String returnValue = (String) request.get(ApiMetadata.RETURN_VALUES);
            Set<String> touchedPaths = null;
            if (ApiMetadata.UPDATED_OLD.equals(returnValue) || ApiMetadata.UPDATED_NEW.equals(
                returnValue)) {
                touchedPaths =
                    CommonServiceUtils.getTouchedPathsFromUpdateDoc(statementInfo.updateDoc);
                if (touchedPaths.isEmpty()) {
                    throw new PhoenixServiceException(
                        "UpdateItem with UPDATED_OLD/UPDATED_NEW requires a non-empty "
                            + "UpdateExpression or AttributeUpdates");
                }
            }

            Map<String, Object> res = DMLUtils.executeUpdate(statementInfo.stmt,
                    returnValue,
                    (String) request.get(ApiMetadata.RETURN_VALUES_ON_CONDITION_CHECK_FAILURE),
                    hasCondExp, statementInfo.canEvaluateUpdateExprOnEmptyDoc, pkCols,
                    ApiOperation.UPDATE_ITEM, touchedPaths);
            res.put(ApiMetadata.CONSUMED_CAPACITY,
                    CommonServiceUtils.getConsumedCapacity((String)request.get(ApiMetadata.TABLE_NAME)));
            return res;
        } catch (SQLException e) {
            throw new PhoenixServiceException(e);
        }
    }

    private static StatementInfo getPreparedStatement(Connection conn, Map<String, Object> request,
            List<PColumn> pkCols) throws SQLException {
        String tableName = (String) request.get(ApiMetadata.TABLE_NAME);
        String condExpr = (String) request.get(ApiMetadata.CONDITION_EXPRESSION);
        Map<String, String> exprAttrNames =
                (Map<String, String>) request.get(ApiMetadata.EXPRESSION_ATTRIBUTE_NAMES);
        Map<String, Object> exprAttrVals =
                (Map<String, Object>) request.get(ApiMetadata.EXPRESSION_ATTRIBUTE_VALUES);

        // Handle legacy Expected parameter conversion to ConditionExpression
        Map<String, Object> expected = (Map<String, Object>) request.get(ApiMetadata.EXPECTED);
        String conditionalOperator = (String) request.get(ApiMetadata.CONDITIONAL_OPERATOR);
        if (condExpr == null && expected != null) {
            // Initialize maps if they don't exist (for Expected conversion)
            if (exprAttrNames == null) {
                exprAttrNames = new HashMap<>();
            }
            if (exprAttrVals == null) {
                exprAttrVals = new HashMap<>();
            }
            // Convert Expected to ConditionExpression
            condExpr = CommonServiceUtils.convertExpectedToConditionExpression(expected,
                    conditionalOperator, exprAttrNames, exprAttrVals);
        }

        String updateExpression = (String) request.get(ApiMetadata.UPDATE_EXPRESSION);
        Map<String, Object> attributeUpdates =
                (Map<String, Object>) request.get(ApiMetadata.ATTRIBUTE_UPDATES);
        BsonDocument updateDoc;
        if (updateExpression != null) {
            try {
                updateDoc = CommonServiceUtils.getBsonUpdateExpressionFromMap(updateExpression,
                        exprAttrNames, exprAttrVals);
            } catch (IllegalArgumentException e) {
                throw new ValidationException(e.getMessage());
            }

        } else {
            updateDoc = CommonServiceUtils.getBsonUpdateExpressionFromAttributeUpdates(
                    attributeUpdates);
        }

        // Extract $SET and $ADD portion from updateDoc for VALUES() clause
        boolean canEvaluateUpdateExprOnEmptyDoc = true;
        BsonDocument newItemDoc = new BsonDocument();
        try {
            updateNewItemDoc(newItemDoc, updateDoc, request, pkCols);
        } catch (BsonUpdateInvalidArgumentException e) {
            canEvaluateUpdateExprOnEmptyDoc = false;
        }

        // Determine query format to use
        QueryFormatInfo formatInfo =
                determineQueryFormat(condExpr, exprAttrNames, pkCols.size(), canEvaluateUpdateExprOnEmptyDoc);

        BsonDocument conditionDoc = null;
        if (!StringUtils.isEmpty(condExpr)) {
            conditionDoc = CommonServiceUtils.getBsonConditionExpressionDoc(condExpr, exprAttrNames,
                    exprAttrVals);
        }

        PreparedStatement stmt =
                conn.prepareStatement(String.format(formatInfo.queryFormat, PhoenixUtils.getFullTableName(tableName, true)));
        return new StatementInfo(stmt, conditionDoc, updateDoc, newItemDoc, formatInfo.needsValuesDoc, canEvaluateUpdateExprOnEmptyDoc);
    }

    /**
     * Extract values from the update document and apply on an empty document to use in VALUES() clause for new row creation.
     * This includes SET operations, ADD operations (for new item creation), and primary keys.
     * For DynamoDB compatibility, ADD operations should contribute to initial values when creating new items.
     */
    private static void updateNewItemDoc(BsonDocument newItemDoc,
                                         BsonDocument updateDoc,
                                         Map<String, Object> request,
                                         List<PColumn> pkCols) {
        BsonDocument newUpdateDoc = new BsonDocument();

        // Add primary key values to the set document
        addKeysToNewItemDoc(newItemDoc, request, pkCols);

        if (updateDoc != null) {
            // Add SET operations - these always contribute to new item creation
            if (updateDoc.containsKey("$SET")) {
                BsonDocument setBsonDoc = updateDoc.getDocument("$SET");
                newUpdateDoc.put("$SET", setBsonDoc);
            }

            // Add ADD operations - for new item creation, these become initial values
            // DynamoDB semantics: ADD on non-existing item creates item with ADD value
            if (updateDoc.containsKey("$ADD")) {
                BsonDocument addBsonDoc = updateDoc.getDocument("$ADD");
                newUpdateDoc.put("$ADD", addBsonDoc);
            }

            // Note: REMOVE and DELETE operations don't contribute to new item creation
            // They are no-ops on non-existing items, handled by the update expression
        }
        UpdateExpressionUtils.updateExpression(newUpdateDoc, newItemDoc);
    }

    /**
     * Helper method to add primary key values to the set document.
     */
    private static void addKeysToNewItemDoc(BsonDocument newItemDoc, Map<String, Object> request,
            List<PColumn> pkCols) {
        Map<String, Object> keyMap = (Map<String, Object>) request.get(ApiMetadata.KEY);
        if (keyMap != null) {
            for (PColumn pkCol : pkCols) {
                String keyName = pkCol.getName().getString();
                Object keyValue = keyMap.get(keyName);
                if (keyValue != null) {
                    BsonValue value =
                            MapToBsonDocument.getValueFromMapVal((Map<String, Object>) keyValue);
                    newItemDoc.put(keyName, value);
                }
            }
        }
    }

    /**
     * Set values on the prepared statement: keys, $SET document (if present),
     * condition and update documents.
     */
    private static void setValuesOnPreparedStatement(StatementInfo statementInfo,
            List<PColumn> pkCols, Map<String, Object> request) throws SQLException {
        
        DMLUtils.setKeysOnStatement(statementInfo.stmt, pkCols,
                (Map<String, Object>) request.get(ApiMetadata.KEY));
        int paramIndex = pkCols.size() + 1;

        // Set the document for VALUES() clause (only for UPDATE flavors, not UPDATE_ONLY)
        // This includes SET operations, ADD operations (for new items), and keys
        if (statementInfo.needsValuesDoc) {
            statementInfo.stmt.setObject(paramIndex++, statementInfo.newItemDoc);
        }

        if (statementInfo.conditionDoc != null) {
            statementInfo.stmt.setObject(paramIndex++, statementInfo.conditionDoc);
        }
        statementInfo.stmt.setObject(paramIndex, statementInfo.updateDoc);
    }

    /**
     * Determine the appropriate query format based on conditions and operations.
     */
    private static QueryFormatInfo determineQueryFormat(String condExpr,
            Map<String, String> exprAttrNames, int pkColsSize, boolean canEvaluateUpdateExprOnEmptyDoc) {

        boolean hasCondition = !StringUtils.isEmpty(condExpr);
        boolean canCreateNewItemWithCondition = false;

        if (hasCondition) {
            // Evaluate if condition can be satisfied on non-existing item
            canCreateNewItemWithCondition = CommonServiceUtils.evaluateConditionOnNonExistingItem(condExpr, exprAttrNames);
        }

        if (canCreateNewItemWithCondition && canEvaluateUpdateExprOnEmptyDoc) {
            // Can create new item and have values to insert (set/add or even just keys)
            String format = pkColsSize == 1 ?
                            CONDITIONAL_UPDATE_WITH_HASH_KEY :
                            CONDITIONAL_UPDATE_WITH_HASH_SORT_KEY;
            return new QueryFormatInfo(format, true);
        } else {
            if (hasCondition) {
                // Cannot create new item (condition prevents it) - only update existing
                String format = (pkColsSize == 1) ?
                        CONDITIONAL_UPDATE_ONLY_WITH_HASH_KEY :
                        CONDITIONAL_UPDATE_ONLY_WITH_HASH_SORT_KEY;
                return new QueryFormatInfo(format, false); // UPDATE_ONLY doesn't use VALUES document
            } else {
                // there was no condition to begin with, still allow creation
                if (canEvaluateUpdateExprOnEmptyDoc) {
                    String format = (pkColsSize == 1) ?
                            UPDATE_WITH_HASH_KEY :
                            UPDATE_WITH_HASH_SORT_KEY;
                    return new QueryFormatInfo(format, true);
                } else {
                    String format = (pkColsSize == 1) ?
                            UPDATE_ONLY_WITH_HASH_KEY :
                            UPDATE_ONLY_WITH_HASH_SORT_KEY;
                    return new QueryFormatInfo(format, false);
                }
            }
        }
    }

    /**
     * Helper class to return query format and whether it needs a VALUES document parameter.
     */
    private static class QueryFormatInfo {
        final String queryFormat;
        final boolean needsValuesDoc;

        QueryFormatInfo(String queryFormat, boolean needsValuesDoc) {
            this.queryFormat = queryFormat;
            this.needsValuesDoc = needsValuesDoc;
        }
    }

    /**
     * Helper class to return statement information including condition, update, and set documents.
     */
    private static class StatementInfo {

        final PreparedStatement stmt;
        final BsonDocument conditionDoc;
        final BsonDocument updateDoc;
        final BsonDocument newItemDoc;
        final boolean needsValuesDoc;
        final boolean canEvaluateUpdateExprOnEmptyDoc;

        public StatementInfo(PreparedStatement stmt, BsonDocument conditionDoc,
                BsonDocument updateDoc, BsonDocument newItemDoc, boolean needsValuesDoc,
                             boolean canEvaluateUpdateExprOnEmptyDoc) {
            this.stmt = stmt;
            this.conditionDoc = conditionDoc;
            this.updateDoc = updateDoc;
            this.newItemDoc = newItemDoc;
            this.needsValuesDoc = needsValuesDoc;
            this.canEvaluateUpdateExprOnEmptyDoc = canEvaluateUpdateExprOnEmptyDoc;
        }
    }
}
