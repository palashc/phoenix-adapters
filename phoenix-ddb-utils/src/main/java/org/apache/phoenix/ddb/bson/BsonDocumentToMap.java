package org.apache.phoenix.ddb.bson;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonNumber;
import org.bson.BsonString;
import org.bson.BsonValue;

public final class BsonDocumentToMap {

    /**
     * Project the given {@link BsonDocument} down to the supplied attribute paths and convert the
     * result into the ddb attribute value map.
     *
     * @param bsonDocument        the source document
     * @param attributesToProject paths to retain
     * @return the DDB AttributeValue map containing only the requested subtrees
     */
    public static Map<String, Object> getProjectedItem(final BsonDocument bsonDocument,
        Collection<String> attributesToProject) {
        return getProjectedItem(bsonDocument, attributesToProject, false);
    }

    public static Map<String, Object> getProjectedItem(final BsonDocument bsonDocument,
        Collection<String> attributesToProject, boolean emitWholeTopLevelAttribute) {
        if (attributesToProject == null || attributesToProject.isEmpty()) {
            return getFullItem(bsonDocument);
        }
        BsonDocument newDocument = new BsonDocument();
        for (String attribute : attributesToProject) {
            if (emitWholeTopLevelAttribute) {
                String topLevel = topLevelSegment(attribute);
                if (newDocument.containsKey(topLevel)) {
                    continue;
                }
                BsonDocument probe = new BsonDocument();
                BsonDocumentConversionUtil.updateNewBsonDocumentByFieldKeyValue(attribute,
                    bsonDocument, probe);
                BsonDocumentConversionUtil.removeNullListElements(probe);
                if (probe.containsKey(topLevel)) {
                    newDocument.put(topLevel, bsonDocument.get(topLevel));
                }
            } else {
                BsonDocumentConversionUtil.updateNewBsonDocumentByFieldKeyValue(attribute,
                    bsonDocument, newDocument);
            }
        }
        BsonDocumentConversionUtil.removeNullListElements(newDocument);
        return getFullItem(newDocument);
    }

    private static String topLevelSegment(String path) {
        int dot = path.indexOf('.');
        int bracket = path.indexOf('[');
        int boundary = dot < 0 ? bracket : (bracket < 0 ? dot : Math.min(dot, bracket));
        return boundary < 0 ? path : path.substring(0, boundary);
    }

    public static Map<String, Object> getFullItem(BsonDocument document) {
        Map<String, Object> map = new HashMap<>();
        updateMapWithDocAttributes(map, document);
        return map;
    }

    private static void updateMapWithDocAttributes(Map<String, Object> map, BsonDocument document) {
        if (document != null) {
            document.forEach((key, value) -> map.put(key, getValueFromBsonVal(value)));
        }
    }

    private static Map<String, Object> getValueFromBsonVal(BsonValue value) {
        Map<String, Object> valueMap = new HashMap<>();
        if (value.isString()) {
            valueMap.put("S", ((BsonString) value).getValue());
        } else if (value.isNumber() || value.isDecimal128()) {
            valueMap.put("N", BsonNumberConversionUtil.numberToString(
                    BsonNumberConversionUtil.getNumberFromBsonNumber((BsonNumber) value)));
        } else if (value.isBinary()) {
            valueMap.put("B", Base64.getEncoder().encodeToString(((BsonBinary) value).getData()));
        } else if (value.isBoolean()) {
            valueMap.put("BOOL", ((BsonBoolean) value).getValue());
        } else if (value.isNull()) {
            valueMap.put("NULL", true);
        } else if (value.isDocument()) {
            BsonDocument subDocument = (BsonDocument) value;
            if (subDocument.size() == 1 && subDocument.containsKey("$set")) {
                BsonValue setValue = subDocument.get("$set");
                if (!setValue.isArray()) {
                    throw new IllegalArgumentException("$set is reserved for Set datatype");
                }
                BsonArray bsonArray = (BsonArray) setValue;
                if (bsonArray.isEmpty()) {
                    throw new IllegalArgumentException("Set cannot be empty");
                }
                BsonValue firstElement = bsonArray.get(0);
                if (firstElement.isString()) {
                    List<String> listValues = new ArrayList<>();
                    for (BsonValue arrayVal : bsonArray) {
                        listValues.add(((BsonString) arrayVal).getValue());
                    }
                    valueMap.put("SS", listValues);
                } else if (firstElement.isNumber() || firstElement.isDecimal128()) {
                    List<String> listValues = new ArrayList<>();
                    for (BsonValue arrayVal : bsonArray) {
                        listValues.add(BsonNumberConversionUtil.numberToString(
                                BsonNumberConversionUtil.getNumberFromBsonNumber(
                                        (BsonNumber) arrayVal)));
                    }
                    valueMap.put("NS", listValues);
                } else if (firstElement.isBinary()) {
                    List<String> listValues = new ArrayList<>();
                    for (BsonValue arrayVal : bsonArray) {
                        listValues.add(Base64.getEncoder()
                                .encodeToString(((BsonBinary) arrayVal).getData()));
                    }
                    valueMap.put("BS", listValues);
                }
            } else {
                Map<String, Object> mapValues = new HashMap<>();
                updateMapWithDocAttributes(mapValues, (BsonDocument) value);
                valueMap.put("M", mapValues);
            }
        } else if (value.isArray()) {
            List<Object> listValues = new ArrayList<>();
            updateListWithAttributes(listValues, (BsonArray) value);
            valueMap.put("L", listValues);
        } else {
            throw new IllegalStateException("Unexpected value type: " + value.getBsonType());
        }
        return valueMap;
    }

    private static void updateListWithAttributes(List<Object> listValues, BsonArray bsonArray) {
        for (BsonValue value : bsonArray) {
            listValues.add(getValueFromBsonVal(value));
        }
    }

}
