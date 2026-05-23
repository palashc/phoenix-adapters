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
package org.apache.phoenix.ddb.utils;

import java.util.Objects;

/**
 * AWS-shaped DynamoDB Streams {@code ShardId}: {@code shardId-<partitionStartMs>-<partitionHex>}.
 *
 * <p>The {@code partitionStartMs} half is the HBase region's creation-time epoch millis (used
 * as the pagination cursor in {@code DescribeStream}); the {@code partitionHex} half is the
 * raw region-encoded partition id.
 *
 * <p>Sibling of {@link PhoenixShardIterator}.
 */
public final class PhoenixShardId {

    private static final String DELIM = "-";

    private final long startTimeMs;
    private final String partitionId;

    private PhoenixShardId(long startTimeMs, String partitionId) {
        this.startTimeMs = startTimeMs;
        this.partitionId = partitionId;
    }

    /** Build from already-known parts. */
    public static PhoenixShardId of(long startTimeMs, String partitionId) {
        if (partitionId == null || partitionId.isEmpty()) {
            throw new IllegalArgumentException("partitionId is required");
        }
        return new PhoenixShardId(startTimeMs, partitionId);
    }

    /**
     * Parse an AWS-shaped {@code shardId-<ms>-<hex>}. Throws if the input is null, empty, or
     * not in that shape. For dual-mode parsing that also accepts a raw partition hex, see
     * {@link DdbAdapterCdcUtils#partitionIdFromShardId} /
     * {@link DdbAdapterCdcUtils#partitionStartMsFromShardId}.
     */
    public static PhoenixShardId parse(String shardId) {
        if (shardId == null || shardId.isEmpty()) {
            throw new IllegalArgumentException("ShardId is required");
        }
        if (!shardId.startsWith(DdbAdapterCdcUtils.SHARD_ID_PREFIX)) {
            throw new IllegalArgumentException(
                "ShardId must start with '" + DdbAdapterCdcUtils.SHARD_ID_PREFIX + "': "
                    + shardId);
        }
        String body = shardId.substring(DdbAdapterCdcUtils.SHARD_ID_PREFIX.length());
        int dashIdx = body.indexOf(DELIM);
        if (dashIdx <= 0 || dashIdx == body.length() - 1) {
            throw new IllegalArgumentException(
                "ShardId must be of the form shardId-<startTimeMs>-<partitionHex>: " + shardId);
        }
        long startTimeMs;
        try {
            startTimeMs = Long.parseLong(body.substring(0, dashIdx));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                "ShardId startTimeMs segment is not numeric: " + shardId, e);
        }
        return new PhoenixShardId(startTimeMs, body.substring(dashIdx + 1));
    }

    public long startTimeMs() {
        return startTimeMs;
    }

    public String partitionId() {
        return partitionId;
    }

    /** Serializes back to the AWS-shaped {@code shardId-<ms>-<hex>}. */
    public String toExternalString() {
        return DdbAdapterCdcUtils.SHARD_ID_PREFIX + startTimeMs + DELIM + partitionId;
    }

    @Override
    public String toString() {
        return toExternalString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PhoenixShardId)) return false;
        PhoenixShardId other = (PhoenixShardId) o;
        return startTimeMs == other.startTimeMs && partitionId.equals(other.partitionId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(startTimeMs, partitionId);
    }
}
