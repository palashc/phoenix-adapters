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
package org.apache.phoenix.ddb.kcl;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import org.apache.phoenix.ddb.DDLTestUtils;
import org.junit.Assert;
import org.junit.Test;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

import java.time.Duration;

/**
 * Verifies KCL v1.13's actual contract when {@code processRecords} throws: KCL logs the
 * exception, <b>skips the offending record</b>, advances the checkpoint past it, and
 * continues with the next records. This is at-most-once-on-failure — consumers that need
 * retry must wrap their work in try/catch themselves.
 *
 * <p>Uses {@code MaxRecords(1)} so each batch contains exactly one record, making the
 * skip signal deterministic.
 */
public class KclProcessFailureIT extends KclTestBase {

    @Test(timeout = 120_000)
    public void skipsRecordWhenProcessorThrows() throws Exception {
        String tableName = "failureIT_" + System.currentTimeMillis();
        CreateTableRequest create = DDLTestUtils.addStreamSpecToRequest(
                DDLTestUtils.getCreateTableRequest(tableName, "PK1", ScalarAttributeType.S,
                        "PK2", ScalarAttributeType.N),
                "NEW_AND_OLD_IMAGES");
        DualOracle oracle = lifecycle.track(DualOracle.create(
                phoenixV2, phoenixStreamsV2, localDdb(), create));
        String streamArn = oracle.phoenixStreamArn();

        for (int i = 1; i <= 5; i++) {
            oracle.putRow("K", i, "rec-" + i);
        }

        String appName = KclTestUtils.uniqueApplicationName("failureApp");
        lifecycle.trackAppName(appName);

        RecordingStore store = new RecordingStore();

        // Throw on the first record only; everything after should be delivered normally.
        // The Phoenix-side processor has the throw-once policy; the oracle's processor uses
        // defaults so its DDB-side store sees all 5 records.
        RecordingRecordProcessor.Behavior behavior = RecordingRecordProcessor.Behavior.builder()
                .failurePolicy(RecordingRecordProcessor.throwOnce())
                .build();

        KinesisClientLibConfiguration config = kclConfig(appName, streamArn).withMaxRecords(1);
        lifecycle.track(startWorker(config,
                new RecordingRecordProcessorFactory(store, behavior)));

        // Records 2..5 arrive (4 records); record 1 was skipped after the throw.
        KclTestUtils.awaitConsumerCaughtUp(store, 4, Duration.ofSeconds(90));
        Assert.assertEquals("KCL did not skip the throwing record",
                java.util.Arrays.asList(2, 3, 4, 5),
                KclTestUtils.pk2SequenceForPk1(store, "K"));

        // Behavioral parity: DDB side (no failure injection) saw all 5 records, and the
        // Phoenix-side Worker delivered exactly the records that KCL didn't skip — i.e. the
        // *skipped set* on the Phoenix side is exactly {1}. If phoenix-ddb-rest had a sticky
        // cursor bug, the Phoenix store would either replay record 1 or stall short of 5.
        KclTestUtils.awaitConsumerCaughtUp(oracle.ddbStore(), 5, Duration.ofSeconds(30));
        Assert.assertEquals("DDB oracle delivered wrong record set",
                java.util.Arrays.asList(1, 2, 3, 4, 5),
                KclTestUtils.pk2SequenceForPk1(oracle.ddbStore(), "K"));
    }
}
