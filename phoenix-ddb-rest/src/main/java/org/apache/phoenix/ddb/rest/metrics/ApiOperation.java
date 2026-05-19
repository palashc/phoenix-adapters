package org.apache.phoenix.ddb.rest.metrics;

import org.apache.phoenix.ddb.utils.ApiMetadata;

public enum ApiOperation {
    CREATE_TABLE(ApiMetadata.CREATE_TABLE, "CreateTable"),
    DELETE_TABLE(ApiMetadata.DELETE_TABLE, "DeleteTable"),
    DESCRIBE_TABLE(ApiMetadata.DESCRIBE_TABLE, "DescribeTable"), DESCRIBE_CONTINUOUS_BACKUPS(
            ApiMetadata.DESCRIBE_CONTINUOUS_BACKUPS, "DescribeContinuousBackups"),
    UPDATE_CONTINUOUS_BACKUPS(ApiMetadata.UPDATE_CONTINUOUS_BACKUPS, "UpdateContinuousBackups"),
    LIST_TABLES(ApiMetadata.LIST_TABLES, "ListTables"),
    UPDATE_TABLE(ApiMetadata.UPDATE_TABLE, "UpdateTable"),
    PUT_ITEM(ApiMetadata.PUT_ITEM, "PutItem"),
    UPDATE_ITEM(ApiMetadata.UPDATE_ITEM, "UpdateItem"),
    DELETE_ITEM(ApiMetadata.DELETE_ITEM, "DeleteItem"),
    GET_ITEM(ApiMetadata.GET_ITEM, "GetItem"),
    BATCH_GET_ITEM(ApiMetadata.BATCH_GET_ITEM, "BatchGetItem"),
    BATCH_WRITE_ITEM(ApiMetadata.BATCH_WRITE_ITEM, "BatchWriteItem"),
    QUERY(ApiMetadata.QUERY, "Query"),
    SCAN(ApiMetadata.SCAN, "Scan"),
    UPDATE_TIME_TO_LIVE(ApiMetadata.UPDATE_TIME_TO_LIVE, "UpdateTimeToLive"),
    DESCRIBE_TIME_TO_LIVE(ApiMetadata.DESCRIBE_TIME_TO_LIVE, "DescribeTimeToLive"),
    LIST_STREAMS(ApiMetadata.LIST_STREAMS, "ListStreams"),
    DESCRIBE_STREAM(ApiMetadata.DESCRIBE_STREAM, "DescribeStream"),
    GET_SHARD_ITERATOR(ApiMetadata.GET_SHARD_ITERATOR, "GetShardIterator"),
    GET_RECORDS(ApiMetadata.GET_RECORDS, "GetRecords");

    private final String apiName;
    private final String metricPrefix;

    ApiOperation(String apiName, String metricPrefix) {
        this.apiName = apiName;
        this.metricPrefix = metricPrefix;
    }

    public String getApiName() {
        return apiName;
    }

    public String getSuccessTimeKey() {
        return metricPrefix + "SuccessTime";
    }

    public String getFailureTimeKey() {
        return metricPrefix + "FailureTime";
    }

    public String getSuccessTimeDesc() {
        return "Time duration in milliseconds for successful " + this.apiName;
    }

    public String getFailureTimeDesc() {
        return "Time duration in milliseconds for failed " + this.apiName;
    }

    public static ApiOperation fromApiName(String apiName) {
        for (ApiOperation op : values()) {
            if (op.getApiName().equals(apiName)) {
                return op;
            }
        }
        throw new IllegalArgumentException("Unknown API: " + apiName);
    }
} 