package org.apache.phoenix.ddb.service;

import org.apache.commons.lang3.StringUtils;
import org.apache.phoenix.ddb.ConnectionUtil;
import org.apache.phoenix.ddb.service.exceptions.PhoenixServiceException;
import org.apache.phoenix.ddb.utils.ApiMetadata;
import org.apache.phoenix.ddb.utils.DdbAdapterCdcUtils;
import org.apache.phoenix.ddb.utils.PhoenixShardId;
import org.apache.phoenix.ddb.utils.PhoenixUtils;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.CDCUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.phoenix.ddb.utils.DdbAdapterCdcUtils.MAX_NUM_CHANGES_AT_TIMESTAMP;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CDC_STREAM_NAME;

public class DescribeStreamService {

    private static final Logger LOGGER = LoggerFactory.getLogger(DescribeStreamService.class);
    private static final int MAX_LIMIT = 100;

    // PARTITION_START_TIME is a non-PK column on SYSTEM.CDC_STREAM (PK is
    // (TABLE_NAME, STREAM_NAME, PARTITION_ID, PARENT_PARTITION_ID)). This query on a single
    // Phoenix CDC stream is bounded by the table's total region count (dozens to low hundreds).
    private static final String SELECT_FROM_CDC_STREAM
            = "SELECT PARTITION_ID, PARENT_PARTITION_ID, PARTITION_START_TIME, PARTITION_END_TIME"
            + " FROM " + SYSTEM_CDC_STREAM_NAME
            + " WHERE TABLE_NAME = ? AND STREAM_NAME = ?";

    // ORDER BY PARTITION_START_TIME ASC ensures a parent always precedes its daughters
    // within a page, so the dynamodb-streams-kinesis-adapter can link parent ↔ child in a
    // single pass over the shard list. Without it, daughters whose region hex sorts
    // lex-before the parent's would leave the parent stuck as a "closed leaf".
    private static final String ORDER_BY_AND_LIMIT
            = " ORDER BY PARTITION_START_TIME ASC, PARTITION_ID ASC LIMIT ?";

    // RVC cursor on (PARTITION_START_TIME, PARTITION_ID). Plain `PARTITION_ID > ?`
    // would skip daughters whose hex sorts before the parent's.
    private static final String CURSOR_RVC_PREDICATE
            = " AND (PARTITION_START_TIME, PARTITION_ID) > (?, ?)";

    private static final String CURSOR_LEGACY_PREDICATE
            = " AND PARTITION_ID > ?";

    private static final String PARENT_PARTITION_START_TIMES_QUERY
            = "SELECT PARTITION_ID, PARTITION_START_TIME FROM "
            + SYSTEM_CDC_STREAM_NAME
            + " WHERE TABLE_NAME = ? AND STREAM_NAME = ? AND PARTITION_ID IN (%s)";

    public static Map<String, Object> describeStream(Map<String, Object> request,
        String connectionUrl) {
        String streamArnInput = (String) request.get(ApiMetadata.STREAM_ARN);
        String streamName = DdbAdapterCdcUtils.normalizeStreamName(streamArnInput);
        String streamArn = DdbAdapterCdcUtils.isStreamArn(streamArnInput) ?
            streamArnInput : DdbAdapterCdcUtils.toStreamArn(streamName);
        String exclusiveStartShardId = (String) request.get(ApiMetadata.EXCLUSIVE_START_SHARD_ID);
        Integer limit = (Integer) request.getOrDefault(ApiMetadata.LIMIT, MAX_LIMIT);
        String tableName = DdbAdapterCdcUtils.getTableNameFromStreamName(streamName);
        Map<String, Object> streamDesc;
        try (Connection conn = ConnectionUtil.getConnection(connectionUrl)) {
            streamDesc = getStreamDescriptionObject(conn, tableName, streamName, streamArn);
            String streamStatus = DdbAdapterCdcUtils.getStreamStatus(conn, tableName, streamName);
            streamDesc.put(ApiMetadata.STREAM_STATUS, streamStatus);
            // Always include Shards (empty for non-ENABLED states) to match the AWS contract.
            streamDesc.put(ApiMetadata.SHARDS, new ArrayList<>());
            if (CDCUtil.CdcStreamStatus.ENABLED.getSerializedValue().equals(streamStatus)) {
                PartitionsPage page = loadPartitionsPage(
                        conn, tableName, streamName, exclusiveStartShardId, limit);
                backfillParentStartTimes(conn, tableName, streamName, page);

                List<Map<String, Object>> shards = new ArrayList<>();
                String lastEvaluatedShardId = null;
                for (Map<String, Object> raw : page.rawShards) {
                    Map<String, Object> shard = buildShardMetadata(raw, page.partitionStartTimes);
                    shards.add(shard);
                    lastEvaluatedShardId = (String) shard.get(ApiMetadata.SHARD_ID);
                }
                streamDesc.put(ApiMetadata.SHARDS, shards);
                if (page.rawShards.size() == limit) {
                    streamDesc.put(ApiMetadata.LAST_EVALUATED_SHARD_ID, lastEvaluatedShardId);
                }
            }
        } catch (SQLException e) {
            throw new PhoenixServiceException(e);
        }
        Map<String, Object> result = new HashMap<>();
        result.put(ApiMetadata.STREAM_DESCRIPTION, streamDesc);
        return result;
    }

    /**
     * Build, bind, and execute the paged partitions query. Returns the page's rows along
     * with a partial {@code partitionId -> startTime} index and the set of parent IDs that
     * weren't in this page (callers should resolve those via {@link #backfillParentStartTimes}).
     */
    private static PartitionsPage loadPartitionsPage(Connection conn, String tableName,
            String streamName, String exclusiveStartShardId, int limit) throws SQLException {
        long exclusiveStartMs = -1L;
        String exclusiveStartPartitionId = null;
        String sql = SELECT_FROM_CDC_STREAM;
        if (!StringUtils.isEmpty(exclusiveStartShardId)) {
            if (DdbAdapterCdcUtils.isShardId(exclusiveStartShardId)) {
                PhoenixShardId phoenixShardId = PhoenixShardId.parse(exclusiveStartShardId);
                exclusiveStartMs = phoenixShardId.startTimeMs();
                exclusiveStartPartitionId = phoenixShardId.partitionId();
            } else {
                exclusiveStartPartitionId = exclusiveStartShardId;
            }
            sql += (exclusiveStartMs >= 0) ? CURSOR_RVC_PREDICATE : CURSOR_LEGACY_PREDICATE;
        }
        sql += ORDER_BY_AND_LIMIT;
        LOGGER.debug("Describe Stream Query: {}", sql);

        PartitionsPage page = new PartitionsPage();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            int idx = 1;
            ps.setString(idx++, tableName);
            ps.setString(idx++, streamName);
            if (exclusiveStartMs >= 0) {
                ps.setLong(idx++, exclusiveStartMs);
                ps.setString(idx++, exclusiveStartPartitionId);
            } else if (exclusiveStartPartitionId != null) {
                ps.setString(idx++, exclusiveStartPartitionId);
            }
            ps.setInt(idx, limit);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> raw = new HashMap<>();
                    String partitionId = rs.getString(1);
                    String parentPartitionId = rs.getString(2);
                    long partitionStartTime = rs.getLong(3);
                    long partitionEndTime = rs.getLong(4);
                    raw.put("partitionId", partitionId);
                    raw.put("parentPartitionId", parentPartitionId);
                    raw.put("partitionStartTime", partitionStartTime);
                    raw.put("partitionEndTime", partitionEndTime);
                    page.rawShards.add(raw);
                    page.partitionStartTimes.put(partitionId, partitionStartTime);
                    if (parentPartitionId != null) {
                        page.parentIdsNeeded.add(parentPartitionId);
                    }
                }
            }
        }
        page.parentIdsNeeded.removeAll(page.partitionStartTimes.keySet());
        return page;
    }

    /**
     * Issue a single follow-up {@code WHERE PARTITION_ID IN (...)} query for any parents
     * referenced by the page but not present in it (a parent landed on an earlier page, or
     * was pulled into the page that came via {@code ExclusiveStartShardId}). Updates
     * {@code page.partitionStartTimes} in place.
     */
    private static void backfillParentStartTimes(Connection conn, String tableName,
            String streamName, PartitionsPage page) throws SQLException {
        if (page.parentIdsNeeded.isEmpty()) {
            return;
        }
        String placeholders = String.join(",",
                java.util.Collections.nCopies(page.parentIdsNeeded.size(), "?"));
        String parentQuery = String.format(PARENT_PARTITION_START_TIMES_QUERY, placeholders);
        LOGGER.debug("Parent Partition Start Times Query: {}", parentQuery);
        try (PreparedStatement pps = conn.prepareStatement(parentQuery)) {
            int idx = 1;
            pps.setString(idx++, tableName);
            pps.setString(idx++, streamName);
            for (String parentId : page.parentIdsNeeded) {
                pps.setString(idx++, parentId);
            }
            try (ResultSet prs = pps.executeQuery()) {
                while (prs.next()) {
                    page.partitionStartTimes.put(prs.getString(1), prs.getLong(2));
                }
            }
        }
    }

    /** Captured state of one DescribeStream page before it gets reshaped into the response. */
    private static final class PartitionsPage {
        final List<Map<String, Object>> rawShards = new ArrayList<>();
        final Map<String, Long> partitionStartTimes = new HashMap<>();
        final Set<String> parentIdsNeeded = new HashSet<>();
    }

    /**
     * Return a StreamDescription object for the given tableName and streamName.
     * Populate all attributes except the list of the shards.
     */
    private static Map<String, Object> getStreamDescriptionObject(Connection conn, String tableName,
        String streamName, String streamArn) throws SQLException {
        PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
        PTable table = pconn.getTable(tableName);
        Map<String, Object> streamDesc = new HashMap<>();
        streamDesc.put(ApiMetadata.STREAM_ARN, streamArn);
        streamDesc.put(ApiMetadata.TABLE_NAME, PhoenixUtils.getTableNameFromFullName(tableName, false));
        long creationTS = DdbAdapterCdcUtils.getCDCIndexTimestampFromStreamName(streamName);
        streamDesc.put(ApiMetadata.STREAM_LABEL, DdbAdapterCdcUtils.getStreamLabel(streamName));
        streamDesc.put(ApiMetadata.STREAM_VIEW_TYPE, table.getSchemaVersion());
        streamDesc.put(ApiMetadata.CREATION_REQUEST_DATE_TIME,
                BigDecimal.valueOf(creationTS).movePointLeft(3));
        streamDesc.put(ApiMetadata.KEY_SCHEMA, DdbAdapterCdcUtils.getKeySchemaForRest(table));
        return streamDesc;
    }

    /**
     * Build a Shard response object from a buffered partition row and a precomputed map
     * of {@code partitionId -> partitionStartTime} (including any parents pulled from the
     * batch parent lookup).
     */
    private static Map<String, Object> buildShardMetadata(Map<String, Object> raw,
        Map<String, Long> partitionStartTimes) {
        String partitionId = (String) raw.get("partitionId");
        String parentPartitionId = (String) raw.get("parentPartitionId");
        long partitionStartTime = (Long) raw.get("partitionStartTime");
        long partitionEndTime = (Long) raw.get("partitionEndTime");

        Map<String, Object> shard = new HashMap<>();
        shard.put(ApiMetadata.SHARD_ID,
            DdbAdapterCdcUtils.toShardId(partitionStartTime, partitionId));
        if (parentPartitionId != null) {
            Long parentStartTime = partitionStartTimes.get(parentPartitionId);
            if (parentStartTime != null) {
                shard.put(ApiMetadata.PARENT_SHARD_ID,
                    DdbAdapterCdcUtils.toShardId(parentStartTime, parentPartitionId));
            } else {
                LOGGER.info("Parent partition {} for partition {} is no longer present in "
                    + "SYSTEM.CDC_STREAM (likely TTLed); omitting ParentShardId "
                    + "from the response.", parentPartitionId, partitionId);
            }
        }
        Map<String, Object> seqNumRange = new HashMap<>();
        seqNumRange.put(ApiMetadata.STARTING_SEQUENCE_NUMBER,
            DdbAdapterCdcUtils.getSequenceNumber(partitionStartTime, 0));
        if (partitionEndTime > 0) {
            seqNumRange.put(ApiMetadata.ENDING_SEQUENCE_NUMBER,
                DdbAdapterCdcUtils.getSequenceNumber(partitionEndTime,
                    MAX_NUM_CHANGES_AT_TIMESTAMP - 1));
        }
        shard.put(ApiMetadata.SEQUENCE_NUMBER_RANGE, seqNumRange);
        return shard;
    }
}
