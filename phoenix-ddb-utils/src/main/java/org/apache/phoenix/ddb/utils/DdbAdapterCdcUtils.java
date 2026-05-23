package org.apache.phoenix.ddb.utils;

import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.util.CDCUtil;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CDC_STREAM_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CDC_STREAM_STATUS_NAME;

/**
 * Utility methods to implement DynamoDB Streams abstractions.
 */
public class DdbAdapterCdcUtils {

    /**
     * Support these many different change records at the same timestamp with unique sequence number.
     */
    public static final int OFFSET_LENGTH = 5;
    public static final int MAX_NUM_CHANGES_AT_TIMESTAMP = (int) Math.pow(10, OFFSET_LENGTH);

    public static final String SHARD_ITERATOR_VERSION = "1";
    public static final String SHARD_ITERATOR_DELIM = "|";
    public static final int SHARD_ITERATOR_NUM_PARTS = 3;
    // JSON key names for the inner state payload of shard iterator
    public static final String SI_FIELD_STREAM_TYPE = "streamType";
    public static final String SI_FIELD_PARTITION_ID = "partitionId";
    public static final String SI_FIELD_SEQ_NUM = "seqNum";
    // phoenix/cdc/stream/{tableName}/{cdc object name}/{cdc index timestamp}/{creation datetime}
    public static final String STREAM_NAME_DELIM = "/";
    public static final int STREAM_NAME_NUM_PARTS = 7;
    public static final String STREAM_NAME_PREFIX = "phoenix/cdc/stream/";
    public static final String CDC_OBJECT_PREFIX = "CDC_";

    public static final String STREAM_ARN_REGION = "us-west-2";
    public static final String STREAM_ARN_ACCOUNT_ID = "000000000000";
    public static final String STREAM_ARN_PREFIX =
        "arn:aws:dynamodb:" + STREAM_ARN_REGION + ":" + STREAM_ARN_ACCOUNT_ID + ":table/";
    public static final String STREAM_ARN_INFIX = "/stream/";

    // ShardId format: shardId-<partitionStartMs>-<32-char-hex-partition-id>
    public static final String SHARD_ID_PREFIX = "shardId-";
    public static final String STREAM_LABEL_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS";
    private static final String STREAM_LABEL_PARSE_PATTERN = "uuuu-MM-dd'T'HH:mm:ss.SSS";
    private static final DateTimeFormatter STREAM_LABEL_FORMATTER =
        DateTimeFormatter.ofPattern(STREAM_LABEL_PARSE_PATTERN, Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT).withZone(ZoneOffset.UTC);

    private static final String STREAM_NAME_QUERY
            = "SELECT STREAM_NAME FROM " + SYSTEM_CDC_STREAM_STATUS_NAME
            + " WHERE TABLE_NAME = '%s' AND STREAM_STATUS IN ('"
            + CDCUtil.CdcStreamStatus.ENABLED.getSerializedValue() + "', '"
            + CDCUtil.CdcStreamStatus.ENABLING.getSerializedValue() + "')";

    private static final String STREAM_STATUS_QUERY
            = "SELECT STREAM_STATUS FROM " + SYSTEM_CDC_STREAM_STATUS_NAME
            + " WHERE TABLE_NAME = '%s' AND STREAM_NAME = '%s'";

    private static final String PARTITION_START_TIME_QUERY
            = "SELECT PARTITION_START_TIME FROM " + SYSTEM_CDC_STREAM_NAME
            + " WHERE TABLE_NAME = '%s' AND STREAM_NAME = '%s' AND PARTITION_ID = '%s'";

    private static final String PARTITION_CLOSED_QUERY
            = "SELECT PARTITION_END_TIME FROM " + SYSTEM_CDC_STREAM_NAME
            + " WHERE TABLE_NAME = '%s' AND STREAM_NAME = '%s' AND  PARTITION_ID = '%s'";

    /**
     * Return the KeySchema for the given PTable.
     */
    public static List<KeySchemaElement> getKeySchema(PTable table) {
        List<KeySchemaElement> keySchema = new ArrayList<>();
        List<PColumn> pkCols = table.getPKColumns();
        keySchema.add(KeySchemaElement.builder().attributeName(pkCols.get(0).getName().getString()).keyType(KeyType.HASH).build());
        if (pkCols.size() == 2) {
            keySchema.add(KeySchemaElement.builder().attributeName(pkCols.get(1).getName().getString()).keyType(KeyType.RANGE).build());
        }
        return keySchema;
    }

    public static List<Map<String, Object>> getKeySchemaForRest(PTable table) {
        List<Map<String, Object>> keySchemaList = new ArrayList<>();
        List<PColumn> pkCols = table.getPKColumns();
        Map<String, Object> hashKeyMap = new HashMap<>();
        hashKeyMap.put("AttributeName", pkCols.get(0).getName().getString());
        hashKeyMap.put("KeyType", KeyType.HASH);
        keySchemaList.add(hashKeyMap);
        if (pkCols.size() == 2) {
            Map<String, Object> sortKeyMap = new HashMap<>();
            sortKeyMap.put("AttributeName", pkCols.get(1).getName().getString());
            sortKeyMap.put("KeyType", KeyType.RANGE);
            keySchemaList.add(sortKeyMap);
        }
        return keySchemaList;
    }


    /**
     * Get the STREAM_STATUS from SYSTEM.CDC_STREAM_STATUS for the given tableName and streamName.
     */
    public static String getStreamStatus(Connection conn, String tableName, String streamName)
            throws SQLException {
        String query = String.format(STREAM_STATUS_QUERY, tableName, streamName);
        ResultSet rs = conn.createStatement().executeQuery(query);
        if (rs.next()) {
            return rs.getString(1);
        } else {
            throw new SQLException("No stream was found with streamName = " + streamName);
        }
    }

    /**
     * Get the STREAM_NAME in ENABLED/ENABLING status from SYSTEM.CDC_STREAM_STATUS for the given tableName.
     */
    public static String getEnabledStreamName(Connection conn, String tableName) throws SQLException {
        String query = String.format(STREAM_NAME_QUERY, tableName);
        ResultSet rs = conn.createStatement().executeQuery(query);
        if (rs.next()) {
            return rs.getString(1);
        } else {
            return null;
        }
    }

    /**
     * Parse CDC creation datetime from streamName.
     */
    public static String getStreamLabel(String streamName) {
        return getStreamNameComponent(streamName, 6);
    }

    /**
     * Parse tableName from streamName.
     */
    public static String getTableNameFromStreamName(String streamName) {
        return getStreamNameComponent(streamName, 3);
    }

    /**
     * Parse CDC Object name from streamName.
     */
    public static String getCDCObjectNameFromStreamName(String streamName) {
        return getStreamNameComponent(streamName, 4);
    }

    /**
     * Parse CDC index creation time from streamName.
     */
    public static long getCDCIndexTimestampFromStreamName(String streamName) {
        return Long.parseLong(getStreamNameComponent(streamName, 5));
    }

    /**
     * Detect whether the given identifier is the AWS-shaped stream ARN
     * rather than the internal stream name.
     */
    public static boolean isStreamArn(String s) {
        return s != null && s.startsWith(STREAM_ARN_PREFIX);
    }

    /**
     * Convert the internal stream name to the AWS-shaped ARN
     * (e.g. arn:aws:dynamodb:us-west-2:000000000000:table/MyTable/stream/2024-01-15T10:30:00.000).
     */
    public static String toStreamArn(String streamName) {
        String internalTableName = getTableNameFromStreamName(streamName);
        String bareTableName = PhoenixUtils.getTableNameFromFullName(internalTableName, false);
        String creationDateTime = getStreamLabel(streamName);
        return STREAM_ARN_PREFIX + bareTableName + STREAM_ARN_INFIX + creationDateTime;
    }

    /**
     * Convert the AWS-shaped stream ARN back to the internal stream name.
     */
    public static String fromStreamArn(String streamArn) {
        if (!isStreamArn(streamArn)) {
            throw new IllegalArgumentException("Not a synthetic stream ARN: " + streamArn);
        }
        String body = streamArn.substring(STREAM_ARN_PREFIX.length());
        int infixIdx = body.indexOf(STREAM_ARN_INFIX);
        if (infixIdx < 0) {
            throw new IllegalArgumentException("Stream ARN missing /stream/ segment: " + streamArn);
        }
        String arnTableName = body.substring(0, infixIdx);
        String creationDateTime = body.substring(infixIdx + STREAM_ARN_INFIX.length());
        long cdcIndexTimestamp = parseCreationDateTime(creationDateTime, streamArn);
        String bareTableName = PhoenixUtils.getTableNameFromFullName(arnTableName, false);
        String internalTableName = PhoenixUtils.getFullTableName(bareTableName, false);
        return STREAM_NAME_PREFIX + internalTableName + STREAM_NAME_DELIM
            + CDC_OBJECT_PREFIX + bareTableName + STREAM_NAME_DELIM
            + cdcIndexTimestamp + STREAM_NAME_DELIM + creationDateTime;
    }

    public static String normalizeStreamName(String streamArnOrName) {
        if (streamArnOrName == null || streamArnOrName.isEmpty()) {
            throw new IllegalArgumentException("StreamArn is required");
        }
        return isStreamArn(streamArnOrName) ? fromStreamArn(streamArnOrName) : streamArnOrName;
    }

    /**
     * Encode an internal Phoenix partition into the AWS-shaped {@code ShardId}
     * {@code shardId-<partitionStartMs>-<partitionHex>}. Length stays in the AWS
     * spec range [28, 65] for any positive epoch-millis and the 32-char HBase region
     * encoded partition id.
     *
     * @param partitionStartMs epoch millis of the partition's start time
     * @param partitionHex     HBase region encoded partition id (32-char lowercase hex)
     */
    public static String toShardId(long partitionStartMs, String partitionHex) {
        return PhoenixShardId.of(partitionStartMs, partitionHex).toExternalString();
    }

    /**
     * Detect whether the given identifier is the AWS-shaped {@code ShardId} rather than
     * the raw HBase region encoded partition id.
     */
    public static boolean isShardId(String s) {
        return s != null && s.startsWith(SHARD_ID_PREFIX);
    }

    /**
     * Extract the bare HBase region encoded partition id from either the new
     * AWS-shaped {@code ShardId} ({@code shardId-<ms>-<hex>}) or a raw partition
     * hex string. Dual-mode for backward compatibility with raw-hex callers; new
     * code that knows it has an AWS-shaped shardId should use
     * {@link PhoenixShardId#parse} directly.
     */
    public static String partitionIdFromShardId(String shardIdOrPartitionHex) {
        if (shardIdOrPartitionHex == null || shardIdOrPartitionHex.isEmpty()) {
            throw new IllegalArgumentException("ShardId is required");
        }
        if (!isShardId(shardIdOrPartitionHex)) {
            return shardIdOrPartitionHex;
        }
        return PhoenixShardId.parse(shardIdOrPartitionHex).partitionId();
    }

    /**
     * Extract the {@code partitionStartMs} half of an AWS-shaped {@code ShardId}
     * ({@code shardId-<ms>-<hex>}). Used by {@code DescribeStream} pagination so the
     * composite cursor {@code (startTime, partitionId)} can be reconstructed from a
     * caller-supplied {@code ExclusiveStartShardId}.
     *
     * @return -1 when the input is a raw partition hex (no embedded timestamp),
     *         the epoch-millis otherwise.
     */
    public static long partitionStartMsFromShardId(String shardIdOrPartitionHex) {
        if (shardIdOrPartitionHex == null || shardIdOrPartitionHex.isEmpty()) {
            throw new IllegalArgumentException("ShardId is required");
        }
        if (!isShardId(shardIdOrPartitionHex)) {
            return -1L;
        }
        return PhoenixShardId.parse(shardIdOrPartitionHex).startTimeMs();
    }

    private static long parseCreationDateTime(String creationDateTime, String streamArn) {
        try {
            return STREAM_LABEL_FORMATTER.parse(creationDateTime, Instant::from).toEpochMilli();
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException(
                "Stream ARN label is not a UTC ISO timestamp (" + STREAM_LABEL_FORMAT + "): "
                    + streamArn, e);
        }
    }

    /**
     * Return the stream type for the given table stored in the SCHEMA_VERSION column of the ptable.
     */
    public static String getStreamType(Connection conn, String tableName) throws SQLException {
        PhoenixConnection phoenixConnection = conn.unwrap(PhoenixConnection.class);
        PTable table = phoenixConnection.getTable(
                new PTableKey(phoenixConnection.getTenantId(), tableName));
        return table.getSchemaVersion();
    }

    /**
     * Get the start time for the given partition from SYSTEM.CDC_STREAM
     */
    public static long getPartitionStartTime(Connection conn, String tableName,
                                             String streamName, String partitionId)
            throws SQLException {
        String query = String.format(PARTITION_START_TIME_QUERY, tableName, streamName, partitionId);
        ResultSet rs = conn.createStatement().executeQuery(query);
        if (rs.next()) {
            return rs.getLong(1);
        } else {
            throw new SQLException("Could not find partition for id: " + partitionId);
        }
    }

    /**
     * Return true if a partition was closed after a split.
     */
    public static long getPartitionEndTime(Connection conn, PhoenixShardIterator pIter)
            throws SQLException {
        String streamName = getEnabledStreamName(conn, pIter.getTableName());
        String query = String.format(PARTITION_CLOSED_QUERY, pIter.getTableName(), streamName, pIter.getPartitionId());
        ResultSet rs = conn.createStatement().executeQuery(query);
        if (rs.next()) {
            return rs.getLong(1);
        } else {
            throw new SQLException("Could not find partition for id: " + pIter.getPartitionId());
        }
    }

    /**
     * Build a 21-digit zero-padded sequence number (SequenceNumber min length 21,
     * max 40). The numeric value is identical to the form
     * (timestamp * 10^OFFSET_LENGTH + offset)
     */
    public static String getSequenceNumber(long timestamp, int offset) {
        return String.format("%021d", timestamp * MAX_NUM_CHANGES_AT_TIMESTAMP + offset);
    }

    public static long parseSequenceNumber(String s) {
        if (s == null || s.isEmpty()) {
            throw new IllegalArgumentException("SequenceNumber is required");
        }
        long val;
        try {
            val = Long.parseLong(s);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("SequenceNumber is not numeric: " + s, e);
        }
        if (val < 0) {
            throw new IllegalArgumentException("SequenceNumber must be non-negative: " + s);
        }
        return val;
    }

    /**
     * Generate a globally unique, deterministic eventID for a change stream record.
     * Formula: md5Hex(tableName + "|" + partitionId + "|" + sequenceNumber)
     *
     * @param tableName the table name from the shard iterator
     * @param partitionId the partition (shard) ID
     * @param sequenceNumber the per-event sequence number (any padded length)
     * @return 32-char lowercase hex string
     */
    public static String getEventId(String tableName, String partitionId, String sequenceNumber) {
        long canonicalSeq = parseSequenceNumber(sequenceNumber);
        try {
            String input = tableName + "|" + partitionId + "|" + canonicalSeq;
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] digest = md.digest(input.getBytes(StandardCharsets.UTF_8));
            return String.format("%032x", new BigInteger(1, digest));
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 algorithm not available", e);
        }
    }

    /**
     * Parse the provided stream name and return a particular component.
     * @param streamName stream name
     * @param index zero based index
     * @return
     */
    private static String getStreamNameComponent(String streamName, int index) {
        String[] parts = streamName.split(STREAM_NAME_DELIM);
        if (parts.length != STREAM_NAME_NUM_PARTS) {
            throw new IllegalArgumentException("Stream Name format is not correct: " + streamName);
        }
        return parts[index];
    }
}
