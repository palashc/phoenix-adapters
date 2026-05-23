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

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreamsClientBuilder;
import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient;
import com.amazonaws.services.dynamodbv2.streamsadapter.StreamsWorkerFactory;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.metrics.interfaces.MetricsLevel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.coprocessor.PhoenixMasterObserver;
import org.apache.phoenix.ddb.LocalDynamoDB;
import org.apache.phoenix.ddb.LocalDynamoDbTestBase;
import org.apache.phoenix.ddb.TestUtils;
import org.apache.phoenix.ddb.rest.RESTServer;
import org.apache.phoenix.end2end.ServerMetadataCacheTestImpl;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.jdbc.PhoenixTestDriver;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.ServerUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;

import java.net.URI;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

/**
 * Base class for all KCL v1 integration tests in this package. Owns the HBase mini-cluster,
 * Phoenix driver, embedded {@link LocalDynamoDB}, and {@link RESTServer} lifecycle; exposes
 * Phoenix-side ({@code phoenix*}) and DDB-side ({@code ddb*}) client factories and provides
 * {@link #kclConfig} + {@link #startWorker} for spinning up Workers with test-safe defaults.
 */
public abstract class KclTestBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(KclTestBase.class);

    private static final String AWS_REGION = "us-east-1";
    private static final String FAKE_ACCESS_KEY = "fakeAccessKey";
    private static final String FAKE_SECRET_KEY = "fakeSecretKey";

    private static HBaseTestingUtility utility;
    private static RESTServer restServer;
    private static String jdbcUrl;
    private static String restEndpoint;
    private static String tmpDir;

    @BeforeClass
    public static void startCluster() throws Exception {
        tmpDir = System.getProperty("java.io.tmpdir");

        LocalDynamoDbTestBase.localDynamoDb().start();

        Configuration conf = TestUtils.getConfigForMiniCluster();
        utility = new HBaseTestingUtility(conf);

        Map<String, String> props = Maps.newHashMapWithExpectedSize(3);
        props.put(QueryServices.TASK_HANDLING_INTERVAL_MS_ATTRIB, Long.toString(0));
        props.put(QueryServices.TASK_HANDLING_INITIAL_DELAY_MS_ATTRIB, Long.toString(1000));
        props.put("hbase.coprocessor.master.classes", PhoenixMasterObserver.class.getName());
        setUpConfigForMiniCluster(conf, new ReadOnlyProps(props.entrySet().iterator()));

        utility.startMiniCluster();
        String zkQuorum = "localhost:" + utility.getZkCluster().getClientPort();
        jdbcUrl = PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkQuorum;
        DriverManager.registerDriver(new PhoenixTestDriver());

        restServer = new RESTServer(utility.getConfiguration());
        restServer.run();

        // Probe Phoenix is queryable now that the REST server has bootstrapped it; absorbs
        // the SYSTEM.CATALOG initialization race the first REST request would otherwise hit
        // on a slow CI worker.
        TestUtils.awaitPhoenixReady(jdbcUrl);
        restEndpoint = "http://" + restServer.getServerAddress();
        LOGGER.info("KclTestBase: REST server up at {}, DDB at {}",
                restEndpoint, LocalDynamoDbTestBase.localDynamoDb().getEndpoint());
    }

    // -------------------------------------------------- per-test fixtures

    /** Phoenix-side V2 writer (system under test). */
    protected DynamoDbClient phoenixV2;
    /** Phoenix-side V2 streams client for ARN resolution and direct introspection. */
    protected DynamoDbStreamsClient phoenixStreamsV2;
    /** Phoenix-side V1 client for inspecting and cleaning the KCL lease table. */
    protected AmazonDynamoDB phoenixV1;
    /** Tracks Workers + app names + oracles for {@link #afterTest()} cleanup. */
    protected final TestLifecycle lifecycle = new TestLifecycle();

    @Before
    public void beforeTest() {
        phoenixV2 = newPhoenixV2Client();
        phoenixStreamsV2 = newPhoenixV2StreamsClient();
        phoenixV1 = newPhoenixV1Client();
    }

    @After
    public void afterTest() {
        lifecycle.tearDown(Duration.ofSeconds(15), phoenixV1);
        try { phoenixV1.shutdown(); } catch (RuntimeException ignored) { /* best effort */ }
        try { phoenixV2.close(); } catch (RuntimeException ignored) { /* best effort */ }
        try { phoenixStreamsV2.close(); } catch (RuntimeException ignored) { /* best effort */ }
    }

    @AfterClass
    public static void stopCluster() throws Exception {
        if (restServer != null) {
            restServer.stop();
        }
        try {
            LocalDynamoDbTestBase.localDynamoDb().stop();
        } catch (RuntimeException e) {
            LOGGER.warn("LocalDynamoDB stop raised: {}", e.toString());
        }
        ServerUtil.ConnectionFactory.shutdown();
        try {
            DriverManager.deregisterDriver(PhoenixDriver.INSTANCE);
        } finally {
            if (utility != null) {
                utility.shutdownMiniCluster();
            }
            ServerMetadataCacheTestImpl.resetCache();
        }
        if (tmpDir != null) {
            System.setProperty("java.io.tmpdir", tmpDir);
        }
    }

    // -------------------------------------------------- exposed test fixtures

    protected static String restEndpoint() {
        return restEndpoint;
    }

    protected static String jdbcUrl() {
        return jdbcUrl;
    }

    // -------------------------------------------------- Phoenix-side client factories
    // All pointed at the in-process REST server (the system under test).

    protected static DynamoDbClient newPhoenixV2Client() {
        return DynamoDbClient.builder()
                .endpointOverride(URI.create(restEndpoint))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(FAKE_ACCESS_KEY, FAKE_SECRET_KEY)))
                .build();
    }

    protected static DynamoDbStreamsClient newPhoenixV2StreamsClient() {
        return DynamoDbStreamsClient.builder()
                .endpointOverride(URI.create(restEndpoint))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(FAKE_ACCESS_KEY, FAKE_SECRET_KEY)))
                .build();
    }

    /** SDK v1 DynamoDB client used by KCL for the lease table (Phoenix-side). */
    protected static AmazonDynamoDB newPhoenixV1Client() {
        return AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(new EndpointConfiguration(restEndpoint, AWS_REGION))
                .withCredentials(fakeCredentials())
                .build();
    }

    /** SDK v1 Streams client (Phoenix-side) wrapped by the KCL adapter. */
    protected static AmazonDynamoDBStreams newPhoenixV1StreamsClient() {
        return AmazonDynamoDBStreamsClientBuilder.standard()
                .withEndpointConfiguration(new EndpointConfiguration(restEndpoint, AWS_REGION))
                .withCredentials(fakeCredentials())
                .build();
    }

    protected static AmazonDynamoDBStreamsAdapterClient newPhoenixAdapterClient() {
        return new AmazonDynamoDBStreamsAdapterClient(newPhoenixV1StreamsClient());
    }

    // -------------------------------------------------- DDB-side client factories
    // All pointed at the embedded LocalDynamoDB server (the AWS-shape reference used by
    // DualOracle to run a parallel KCL Worker for differential testing).

    /** Singleton LocalDynamoDB started in {@link #startCluster()}. */
    protected static LocalDynamoDB localDdb() {
        return LocalDynamoDbTestBase.localDynamoDb();
    }

    /**
     * SDK v1 DynamoDB client pointed at the embedded DDB server. Uses
     * {@code dummykey}/{@code dummysecret} (the same credentials the V2 client builder
     * uses): LocalDynamoDB partitions its in-memory namespace by access-key, so a V1
     * client signing with a different key would see an empty namespace and fail to find
     * streams just created via V2.
     */
    public static AmazonDynamoDB newDdbV1Client(LocalDynamoDB local) {
        return AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(
                        new EndpointConfiguration(local.getEndpoint(), AWS_REGION))
                .withCredentials(ddbCredentials())
                .build();
    }

    /** SDK v1 Streams client pointed at the embedded DDB server. */
    public static AmazonDynamoDBStreams newDdbV1StreamsClient(LocalDynamoDB local) {
        return AmazonDynamoDBStreamsClientBuilder.standard()
                .withEndpointConfiguration(
                        new EndpointConfiguration(local.getEndpoint(), AWS_REGION))
                .withCredentials(ddbCredentials())
                .build();
    }

    public static AmazonDynamoDBStreamsAdapterClient newDdbAdapterClient(LocalDynamoDB local) {
        return new AmazonDynamoDBStreamsAdapterClient(newDdbV1StreamsClient(local));
    }

    private static AWSCredentialsProvider ddbCredentials() {
        return new AWSStaticCredentialsProvider(new BasicAWSCredentials("dummykey", "dummysecret"));
    }

    /** Mocked CloudWatch — KCL rejects null even with {@code MetricsLevel.NONE}. */
    protected static AmazonCloudWatch newMockCloudWatch() {
        return Mockito.mock(AmazonCloudWatch.class);
    }

    protected static AWSCredentialsProvider fakeCredentials() {
        return new AWSStaticCredentialsProvider(new BasicAWSCredentials(FAKE_ACCESS_KEY, FAKE_SECRET_KEY));
    }

    // -------------------------------------------------- KCL configuration

    /**
     * KCL config seeded with test-safe defaults: no CloudWatch, fast failover, fast shard
     * sync. Per-test knobs (idle time, max records, initial position) are chained on the
     * returned object.
     */
    protected static KinesisClientLibConfiguration kclConfig(String applicationName, String streamArn) {
        String workerId = applicationName + "-" + UUID.randomUUID();
        return new KinesisClientLibConfiguration(
                    applicationName, streamArn, fakeCredentials(), workerId)
                .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON)
                .withMetricsLevel(MetricsLevel.NONE)
                .withFailoverTimeMillis(2000)
                .withShardSyncIntervalMillis(2000)
                .withParentShardPollIntervalMillis(500)
                .withIdleTimeBetweenReadsInMillis(250)
                .withMaxRecords(10);
    }

    // -------------------------------------------------- Worker lifecycle

    /** Start a Worker on a fresh executor with default adapter/lease/CW wiring. */
    protected WorkerHandle startWorker(
            KinesisClientLibConfiguration config,
            IRecordProcessorFactory factory) {
        return startWorker(config, factory,
                newPhoenixAdapterClient(), newPhoenixV1Client(), newMockCloudWatch(),
                Executors.newCachedThreadPool());
    }

    /** Full-control overload for tests that need to share an executor across Workers. */
    protected WorkerHandle startWorker(
            KinesisClientLibConfiguration config,
            IRecordProcessorFactory factory,
            AmazonDynamoDBStreamsAdapterClient adapter,
            AmazonDynamoDB leaseDdb,
            AmazonCloudWatch cw,
            ExecutorService executor) {
        return startWorkerStatic(config, factory, adapter, leaseDdb, cw, executor);
    }

    /**
     * Static escape hatch — used by {@link DualOracle} (same package, not a subclass) to
     * spin up its own KCL Worker against the embedded DDB without inheriting from this class.
     */
    public static WorkerHandle startWorkerStatic(
            KinesisClientLibConfiguration config,
            IRecordProcessorFactory factory,
            AmazonDynamoDBStreamsAdapterClient adapter,
            AmazonDynamoDB leaseDdb,
            AmazonCloudWatch cw,
            ExecutorService executor) {

        Worker worker = StreamsWorkerFactory.createDynamoDbStreamsWorker(
                factory, config, adapter, leaseDdb, cw, executor);

        Future<?> running = executor.submit(worker);
        return new WorkerHandle(worker, executor, running);
    }

    /** Handle to a running Worker supporting graceful and forced shutdown. */
    public static final class WorkerHandle {
        private final Worker worker;
        private final ExecutorService executor;
        private final Future<?> running;

        WorkerHandle(Worker worker, ExecutorService executor, Future<?> running) {
            this.worker = worker;
            this.executor = executor;
            this.running = running;
        }

        /**
         * Drain in-flight batches and notify processors of graceful shutdown. Blocks until
         * done or {@code timeout} elapses.
         *
         * <p>Two non-obvious behaviors callers need to know:
         * <ol>
         *   <li>The Future completes when KCL reaches "lease-tracking stopped" — which is
         *       BEFORE the processor's shutdown-notification checkpoint UpdateItem necessarily
         *       reaches the lease table. Callers that need the checkpoint to be durable must
         *       poll the lease table after this returns.</li>
         *   <li>We deliberately do NOT shut down the executor here — touching it even via
         *       {@code shutdown()} can preempt the in-flight checkpoint RPC. Cached pool
         *       threads die on idle keepalive; the executor closes when the JVM exits.</li>
         * </ol>
         */
        public void gracefulShutdown(Duration timeout) {
            try {
                Future<Boolean> done = worker.startGracefulShutdown();
                done.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            } catch (TimeoutException te) {
                LOGGER.warn("Graceful shutdown timed out after {}, falling back to shutdown()",
                        timeout);
                worker.shutdown();
            } catch (Exception e) {
                LOGGER.warn("Graceful shutdown raised; falling back to shutdown()", e);
                worker.shutdown();
            }
        }

        /** Hard stop — used when a test wants to simulate a process crash mid-stream. */
        public void shutdownNow() {
            worker.shutdown();
            executor.shutdownNow();
            try {
                executor.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }

        public boolean isDone() {
            return running.isDone();
        }
    }

    /** Per-test bookkeeping for Workers, oracles, and app names so {@code @After} can clean up. */
    protected static final class TestLifecycle {
        private final List<WorkerHandle> handles = new ArrayList<>();
        private final List<String> applicationNames = new ArrayList<>();
        private final List<DualOracle> oracles = new ArrayList<>();

        public WorkerHandle track(WorkerHandle h) {
            handles.add(h);
            return h;
        }

        public DualOracle track(DualOracle oracle) {
            oracles.add(oracle);
            return oracle;
        }

        public void trackAppName(String appName) {
            applicationNames.add(appName);
        }

        public void tearDown(Duration shutdownTimeout, AmazonDynamoDB phoenixV1) {
            for (WorkerHandle h : handles) {
                if (!h.isDone()) {
                    h.gracefulShutdown(shutdownTimeout);
                }
            }
            for (DualOracle o : oracles) {
                try { o.close(); } catch (RuntimeException e) {
                    LOGGER.warn("Oracle close raised: {}", e.toString());
                }
            }
            for (String app : applicationNames) {
                KclTestUtils.deleteLeaseTableQuietly(phoenixV1, app);
            }
        }
    }
}
