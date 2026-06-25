package org.apache.phoenix.ddb.service.utils;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.phoenix.ddb.service.exceptions.ValidationException;
import org.bson.RawBsonDocument;

import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.ddb.bson.BsonDocumentToMap;
import org.apache.phoenix.ddb.service.exceptions.ConditionCheckFailedException;
import org.apache.phoenix.ddb.utils.ApiMetadata;
import org.apache.phoenix.ddb.rest.metrics.ApiOperation;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PVarbinaryEncoded;
import org.apache.phoenix.schema.types.PVarchar;

public class DMLUtils {


    /**
     * Extract values for keys from the item and set them on the PreparedStatement.
     */
    public static void setKeysOnStatement(PreparedStatement stmt, List<PColumn> pkCols,
        Map<String, Object> item) throws SQLException {
        for (int i = 0; i < pkCols.size(); i++) {
            PColumn pkCol = pkCols.get(i);
            String colName = pkCol.getName().toString();
            PDataType type = pkCol.getDataType();
            if (type == PDouble.INSTANCE) {
                String strValue = (String) ((Map<String, Object>) item.get(colName)).get("N");
                double value = Double.parseDouble(strValue);
                stmt.setDouble(i + 1, value);
            } else if (type == PVarchar.INSTANCE) {
                String value = (String) ((Map<String, Object>) item.get(colName)).get("S");
                stmt.setString(i + 1, value);
            } else if (type == PVarbinaryEncoded.INSTANCE) {
                String value = (String) ((Map<String, Object>) item.get(colName)).get("B");
                byte[] b = Base64.getDecoder().decode(value);
                stmt.setBytes(i + 1, b);
            } else {
                throw new IllegalArgumentException(
                    "Primary Key column type " + type + " is not " + "correct type");
            }
        }
    }

    public static Map<String, Object> executeUpdate(PreparedStatement stmt, String returnValue,
        String returnValuesOnConditionCheckFailure, boolean hasCondExp,
        boolean canEvaluateExprOnEmptyDoc, List<PColumn> pkCols, ApiOperation apiOperation)
        throws SQLException, ConditionCheckFailedException {
        return executeUpdate(stmt, returnValue, returnValuesOnConditionCheckFailure, hasCondExp,
            canEvaluateExprOnEmptyDoc, pkCols, apiOperation, null);
    }

    public static Map<String, Object> executeUpdate(PreparedStatement stmt, String returnValue,
        String returnValuesOnConditionCheckFailure, boolean hasCondExp,
        boolean canEvaluateExprOnEmptyDoc, List<PColumn> pkCols, ApiOperation apiOperation,
        Set<String> updatedAttributePaths) throws SQLException, ConditionCheckFailedException {
        try {
            Map<String, Object> returnAttrs = new HashMap<>();
            if (!needReturnRow(returnValue, returnValuesOnConditionCheckFailure)) {
                int returnStatus = stmt.executeUpdate();
                if (returnStatus == 0) {
                    if (apiOperation == ApiOperation.DELETE_ITEM) {
                        //there was a condition expression which could not be true on empty row
                        if (hasCondExp && !canEvaluateExprOnEmptyDoc) {
                            // Condition definitely fails on empty doc → throw
                            throw new ConditionCheckFailedException();
                        }
                        // TODO: If canEvaluateExprOnEmptyDoc is true, we can't tell here if:
                        // TODO: Row didn't exist (success) vs row existed but condition failed (should throw)
                    } else {
                        if (hasCondExp) {
                            throw new ConditionCheckFailedException();
                        }
                        if (!canEvaluateExprOnEmptyDoc && apiOperation == ApiOperation.UPDATE_ITEM) {
                            throw new ValidationException(
                                    "The provided expression references an attribute that does not exist in the item");
                        }
                    }
                }
                return new HashMap<>();
            }
            Pair<Integer, ResultSet> resultPair;
            if (returnOldImage(returnValue) && apiOperation != ApiOperation.DELETE_ITEM) {
                resultPair =
                    stmt.unwrap(PhoenixPreparedStatement.class).executeAtomicUpdateReturnOldRow();
            } else {
                resultPair =
                    stmt.unwrap(PhoenixPreparedStatement.class).executeAtomicUpdateReturnRow();
            }
            int returnStatus = resultPair.getFirst();
            ResultSet rs = resultPair.getSecond();
            RawBsonDocument rawBsonDocument =
                rs == null ? null : (RawBsonDocument) rs.getObject(pkCols.size() + 1);
            if ((returnStatus == 0 && apiOperation != ApiOperation.DELETE_ITEM) ||
                (apiOperation == ApiOperation.DELETE_ITEM && rawBsonDocument == null
                        && !canEvaluateExprOnEmptyDoc)) {
                if (hasCondExp) {
                    ConditionCheckFailedException conditionalCheckFailedException =
                        new ConditionCheckFailedException();
                    if (ApiMetadata.ALL_OLD.equals(returnValuesOnConditionCheckFailure) &&
                        apiOperation != ApiOperation.DELETE_ITEM) {
                        if (rawBsonDocument != null) {
                            conditionalCheckFailedException.setItem(
                                    BsonDocumentToMap.getFullItem(rawBsonDocument));
                        }
                    }
                    throw conditionalCheckFailedException;
                }
                if (!canEvaluateExprOnEmptyDoc && apiOperation == ApiOperation.UPDATE_ITEM) {
                    throw new ValidationException(
                            "The provided expression references an attribute that does not exist in the item");
                }
            } else {
                boolean returnValuesInResponse = false;
                if (apiOperation != ApiOperation.DELETE_ITEM) {
                    if (returnValueInResp(returnValue)) {
                        returnValuesInResponse = true;
                    }
                } else if (ApiMetadata.ALL_OLD.equals(returnValue) && rawBsonDocument != null) {
                    returnValuesInResponse = true;
                }
                if (returnValuesInResponse) {
                    boolean projectTouchedPaths = ApiMetadata.UPDATED_OLD.equals(returnValue)
                        || ApiMetadata.UPDATED_NEW.equals(returnValue);
                    Map<String, Object> attrMap;
                    if (projectTouchedPaths) {
                        Objects.requireNonNull(updatedAttributePaths,
                            "UPDATED_OLD/UPDATED_NEW requires a non-null touched-path set");
                        attrMap = rawBsonDocument == null ?
                            new HashMap<>() :
                            BsonDocumentToMap.getProjectedItem(rawBsonDocument,
                                updatedAttributePaths, true);
                    } else {
                        attrMap = BsonDocumentToMap.getFullItem(rawBsonDocument);
                    }
                    returnAttrs = new HashMap<>();
                    returnAttrs.put(ApiMetadata.ATTRIBUTES, attrMap);
                }
            }
            return returnAttrs;
        } catch (SQLException e) {
            if (e.getMessage() != null && e.getMessage()
                .contains("BsonUpdateInvalidArgumentException")) {
                throw new ValidationException("Invalid document path used for update");
            }
            throw e;
        }
    }

    private static boolean returnValueInResp(String returnValue) {
        return ApiMetadata.ALL_NEW.equals(returnValue) || ApiMetadata.ALL_OLD.equals(returnValue)
            || ApiMetadata.UPDATED_OLD.equals(returnValue) || ApiMetadata.UPDATED_NEW.equals(
            returnValue);
    }

    private static boolean returnOldImage(String returnValue) {
        return ApiMetadata.ALL_OLD.equals(returnValue) || ApiMetadata.UPDATED_OLD.equals(
            returnValue);
    }

    /**
     * Use return row api only if
     * returnValue is not empty/null and not NONE
     * OR
     * returnValuesOnConditionCheckFailure is not empty/null and not NONE
     */
    private static boolean needReturnRow(String returnValue,
        String returnValuesOnConditionCheckFailure) {
        return (returnValue != null && !returnValue.equals(ApiMetadata.NONE)) || (
            returnValuesOnConditionCheckFailure != null
                && !returnValuesOnConditionCheckFailure.equals(ApiMetadata.NONE));
    }
}