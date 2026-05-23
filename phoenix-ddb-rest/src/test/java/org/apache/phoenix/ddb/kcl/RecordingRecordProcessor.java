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

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IShutdownNotificationAware;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Test {@link IRecordProcessor} that records every record into a {@link RecordingStore} and
 * lets each KCL scenario be expressed by composing a {@link Behavior} rather than subclassing.
 * Pluggable knobs: {@link CheckpointPolicy}, {@link FailurePolicy}, per-batch sleep, and
 * shutdown-checkpoint flag.
 */
public class RecordingRecordProcessor implements IRecordProcessor, IShutdownNotificationAware {

    private static final Logger LOGGER = LoggerFactory.getLogger(RecordingRecordProcessor.class);

    private final RecordingStore store;
    private final Behavior behavior;
    private final AtomicInteger recordsObserved = new AtomicInteger();
    private final AtomicReference<String> shardId = new AtomicReference<>();

    public RecordingRecordProcessor(RecordingStore store, Behavior behavior) {
        this.store = store;
        this.behavior = behavior;
    }

    @Override
    public void initialize(InitializationInput input) {
        shardId.set(input.getShardId());
    }

    @Override
    public void processRecords(ProcessRecordsInput input) {
        IRecordProcessorCheckpointer checkpointer = input.getCheckpointer();

        if (input.getRecords().isEmpty()) {
            behavior.onEmptyBatch(checkpointer);
            return;
        }

        if (behavior.sleepBeforeBatch() != null) {
            sleep(behavior.sleepBeforeBatch());
        }

        Record lastSuccessful = null;
        for (Record record : input.getRecords()) {
            int positionWithinShard = recordsObserved.get();
            try {
                if (behavior.failurePolicy().shouldThrow(positionWithinShard, record)) {
                    throw new RuntimeException(
                            "Injected processing failure at position " + positionWithinShard);
                }
                if (record instanceof RecordAdapter) {
                    com.amazonaws.services.dynamodbv2.model.Record ddb =
                            ((RecordAdapter) record).getInternalObject();
                    store.append(shardId.get(), ddb);
                }
                recordsObserved.incrementAndGet();
                lastSuccessful = record;
            } catch (RuntimeException re) {
                if (!behavior.failurePolicy().swallow()) {
                    // Propagate to KCL. In KCL v1.13 this causes KCL to log + skip the
                    // record (NOT retry the batch); the consumer advances past it.
                    throw re;
                }
                // Swallow-and-advance: count as observed but don't append to the store.
                LOGGER.info("Swallowed failure on shard {} at position {}: {}",
                        shardId.get(), positionWithinShard, re.getMessage());
                recordsObserved.incrementAndGet();
                lastSuccessful = record;
            }

            if (behavior.checkpointPolicy() == CheckpointPolicy.PER_RECORD && lastSuccessful != null) {
                tryCheckpoint(checkpointer, lastSuccessful);
            }
        }

        if (behavior.checkpointPolicy() == CheckpointPolicy.PER_BATCH && lastSuccessful != null) {
            tryCheckpoint(checkpointer, lastSuccessful);
        }
    }

    @Override
    public void shutdown(ShutdownInput input) {
        ShutdownReason reason = input.getShutdownReason();
        // Only TERMINATE checkpoints here: it's required to mark the lease SHARD_END and
        // unblock daughter leases. ZOMBIE is a lost-lease event (checkpointing would race
        // with the new owner). REQUESTED never reaches shutdown() in KCL v1 — see
        // shutdownRequested() below.
        if (reason == ShutdownReason.TERMINATE) {
            try {
                input.getCheckpointer().checkpoint();
            } catch (Exception e) {
                LOGGER.warn("Checkpoint on shutdown ({}) failed for shard {}: {}",
                        reason, shardId.get(), e.toString());
            }
        }
    }

    /**
     * KCL v1 graceful-shutdown checkpoint path. In KCL v1 the {@code shutdown()} callback
     * is never invoked with {@code REQUESTED}; processors that want a final checkpoint on
     * graceful stop must implement {@link IShutdownNotificationAware}.
     */
    @Override
    public void shutdownRequested(IRecordProcessorCheckpointer checkpointer) {
        if (behavior.checkpointOnShutdown()) {
            try {
                checkpointer.checkpoint();
            } catch (Exception e) {
                LOGGER.warn("Checkpoint on shutdownRequested failed for shard {}: {}",
                        shardId.get(), e.toString());
            }
        }
    }

    private static void tryCheckpoint(IRecordProcessorCheckpointer checkpointer, Record record) {
        try {
            checkpointer.checkpoint(record);
        } catch (Exception e) {
            LOGGER.warn("Checkpoint failed at seqNum {}: {}", record.getSequenceNumber(),
                    e.toString());
        }
    }

    private static void sleep(Duration d) {
        try {
            Thread.sleep(d.toMillis());
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    // -------------------------------------------------- behavior contract

    public enum CheckpointPolicy {
        /** Call {@code checkpoint(lastRecord)} after the whole batch. */
        PER_BATCH,
        /** Call {@code checkpoint(record)} after each record. */
        PER_RECORD,
        /** Never checkpoint from processRecords (used by checkpoint-on-shutdown tests). */
        NEVER
    }

    /**
     * Decides whether a record should trigger a thrown exception and, if so, whether the
     * processor catches it (swallow + advance) or lets it propagate (KCL skips the record).
     */
    public interface FailurePolicy {
        boolean shouldThrow(int positionWithinShard, Record record);

        /** If true, exceptions thrown for this record are caught and the record is skipped. */
        boolean swallow();

        FailurePolicy NEVER = new FailurePolicy() {
            @Override public boolean shouldThrow(int position, Record record) { return false; }
            @Override public boolean swallow() { return false; }
        };
    }

    /** Throws on the first record only; the rest pass through. KCL skips the thrown one. */
    public static FailurePolicy throwOnce() {
        AtomicInteger remaining = new AtomicInteger(1);
        return new FailurePolicy() {
            @Override
            public boolean shouldThrow(int position, Record record) {
                return remaining.getAndDecrement() > 0;
            }
            @Override public boolean swallow() { return false; }
        };
    }

    /**
     * Swallow-and-advance: throws on the record at {@code poisonPosition}, catches its own
     * exception, and lets KCL advance past it. Matches the consumer pattern of wrapping
     * {@code processRecords} in try/catch + checkpoint to silently drop poison records.
     */
    public static FailurePolicy poisonAt(int poisonPosition) {
        return new FailurePolicy() {
            @Override
            public boolean shouldThrow(int position, Record record) {
                return position == poisonPosition;
            }
            @Override public boolean swallow() { return true; }
        };
    }

    // -------------------------------------------------- behavior bundle

    /** Immutable policy bundle. Build with {@link Behavior#builder()}. */
    public static final class Behavior {
        private final CheckpointPolicy checkpointPolicy;
        private final FailurePolicy failurePolicy;
        private final Duration sleepBeforeBatch;
        private final boolean checkpointOnShutdown;
        private final java.util.function.Consumer<IRecordProcessorCheckpointer> onEmptyBatch;

        private Behavior(Builder b) {
            this.checkpointPolicy = b.checkpointPolicy;
            this.failurePolicy = b.failurePolicy;
            this.sleepBeforeBatch = b.sleepBeforeBatch;
            this.checkpointOnShutdown = b.checkpointOnShutdown;
            this.onEmptyBatch = b.onEmptyBatch;
        }

        public CheckpointPolicy checkpointPolicy() { return checkpointPolicy; }
        public FailurePolicy failurePolicy()       { return failurePolicy; }
        public Duration sleepBeforeBatch()         { return sleepBeforeBatch; }
        public boolean checkpointOnShutdown()      { return checkpointOnShutdown; }

        void onEmptyBatch(IRecordProcessorCheckpointer ckpt) {
            if (onEmptyBatch != null) {
                onEmptyBatch.accept(ckpt);
            }
        }

        public static Builder builder() {
            return new Builder();
        }

        public static Behavior defaults() {
            return builder().build();
        }

        public static final class Builder {
            private CheckpointPolicy checkpointPolicy = CheckpointPolicy.PER_BATCH;
            private FailurePolicy failurePolicy = FailurePolicy.NEVER;
            private Duration sleepBeforeBatch = null;
            private boolean checkpointOnShutdown = false;
            private java.util.function.Consumer<IRecordProcessorCheckpointer> onEmptyBatch = null;

            public Builder checkpointPolicy(CheckpointPolicy p)   { this.checkpointPolicy = p; return this; }
            public Builder failurePolicy(FailurePolicy p)         { this.failurePolicy = p; return this; }
            public Builder sleepBeforeBatch(Duration d)           { this.sleepBeforeBatch = d; return this; }
            public Builder checkpointOnShutdown(boolean b)        { this.checkpointOnShutdown = b; return this; }
            public Builder onEmptyBatch(
                    java.util.function.Consumer<IRecordProcessorCheckpointer> handler) {
                this.onEmptyBatch = handler;
                return this;
            }

            public Behavior build() {
                return new Behavior(this);
            }
        }
    }
}
