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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Verifies that two KCL Workers running in the same JVM, on a shared {@link ExecutorService},
 * consuming two different streams with independent {@code applicationName}s, deliver each
 * stream's records only to their own processor — no cross-talk and isolated lease tables.
 *
 * <p>Mirrors the common consumer pattern of running one Worker per table inside a single
 * application process.
 */
public class KclMultiStreamSingleJvmIT extends KclTestBase {

    @Test(timeout = 180_000)
    public void twoWorkersTwoStreamsNoCrossTalk() throws Exception {
        String tableA = "multiStreamA_" + System.currentTimeMillis();
        String tableB = "multiStreamB_" + System.currentTimeMillis();
        CreateTableRequest createA = DDLTestUtils.addStreamSpecToRequest(
                DDLTestUtils.getCreateTableRequest(tableA, "PK1", ScalarAttributeType.S,
                        "PK2", ScalarAttributeType.N),
                "NEW_AND_OLD_IMAGES");
        CreateTableRequest createB = DDLTestUtils.addStreamSpecToRequest(
                DDLTestUtils.getCreateTableRequest(tableB, "PK1", ScalarAttributeType.S,
                        "PK2", ScalarAttributeType.N),
                "NEW_AND_OLD_IMAGES");

        DualOracle oracleA = lifecycle.track(DualOracle.create(
                phoenixV2, phoenixStreamsV2, localDdb(), createA));
        DualOracle oracleB = lifecycle.track(DualOracle.create(
                phoenixV2, phoenixStreamsV2, localDdb(), createB));

        String streamArnA = oracleA.phoenixStreamArn();
        String streamArnB = oracleB.phoenixStreamArn();

        String appA = KclTestUtils.uniqueApplicationName("multiStreamAppA");
        String appB = KclTestUtils.uniqueApplicationName("multiStreamAppB");
        lifecycle.trackAppName(appA);
        lifecycle.trackAppName(appB);

        ExecutorService shared = Executors.newCachedThreadPool();

        RecordingStore storeA = new RecordingStore();
        RecordingStore storeB = new RecordingStore();
        RecordingRecordProcessor.Behavior defaults = RecordingRecordProcessor.Behavior.defaults();

        KinesisClientLibConfiguration cfgA = kclConfig(appA, streamArnA);
        KinesisClientLibConfiguration cfgB = kclConfig(appB, streamArnB);

        lifecycle.track(startWorker(cfgA,
                new RecordingRecordProcessorFactory(storeA, defaults),
                newPhoenixAdapterClient(), newPhoenixV1Client(), newMockCloudWatch(), shared));
        lifecycle.track(startWorker(cfgB,
                new RecordingRecordProcessorFactory(storeB, defaults),
                newPhoenixAdapterClient(), newPhoenixV1Client(), newMockCloudWatch(), shared));

        // Interleave writes to both tables.
        for (int i = 1; i <= 5; i++) {
            oracleA.putRow("A", i, "table-A-" + i);
            oracleB.putRow("B", i, "table-B-" + i);
        }

        KclTestUtils.awaitConsumerCaughtUp(storeA, 5, Duration.ofSeconds(60));
        KclTestUtils.awaitConsumerCaughtUp(storeB, 5, Duration.ofSeconds(60));

        // No cross-talk: storeA must contain only PK1=A; storeB only PK1=B.
        for (com.amazonaws.services.dynamodbv2.model.Record r : storeA.allRecordsInArrival()) {
            Assert.assertEquals("storeA leaked a non-A record", "A", KclTestUtils.pk1Of(r));
        }
        for (com.amazonaws.services.dynamodbv2.model.Record r : storeB.allRecordsInArrival()) {
            Assert.assertEquals("storeB leaked a non-B record", "B", KclTestUtils.pk1Of(r));
        }

        // Lease tables exist independently — each app has its own.
        Assert.assertFalse("appA lease table is empty/missing",
                KclTestUtils.scanLeases(phoenixV1, appA).isEmpty());
        Assert.assertFalse("appB lease table is empty/missing",
                KclTestUtils.scanLeases(phoenixV1, appB).isEmpty());

        // Behavioral parity: same no-cross-talk property must hold on the DDB side. If
        // phoenix-ddb-rest had a stream-routing bug where Stream A's GetRecords leaked
        // Stream B's records, the Phoenix-side stores would have crossed contamination but
        // the DDB-side stores wouldn't.
        KclTestUtils.awaitConsumerCaughtUp(oracleA.ddbStore(), 5, Duration.ofSeconds(30));
        KclTestUtils.awaitConsumerCaughtUp(oracleB.ddbStore(), 5, Duration.ofSeconds(30));
        for (com.amazonaws.services.dynamodbv2.model.Record r : oracleA.ddbStore().allRecordsInArrival()) {
            Assert.assertEquals("DDB oracle A leaked non-A record", "A", KclTestUtils.pk1Of(r));
        }
        for (com.amazonaws.services.dynamodbv2.model.Record r : oracleB.ddbStore().allRecordsInArrival()) {
            Assert.assertEquals("DDB oracle B leaked non-B record", "B", KclTestUtils.pk1Of(r));
        }
    }
}
