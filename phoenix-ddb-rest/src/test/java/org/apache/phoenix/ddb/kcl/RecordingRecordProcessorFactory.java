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

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;

/**
 * KCL-required factory that hands out one {@link RecordingRecordProcessor} per shard, all
 * writing to the same {@link RecordingStore} and configured with the same {@link
 * RecordingRecordProcessor.Behavior}.
 */
public class RecordingRecordProcessorFactory implements IRecordProcessorFactory {

    private final RecordingStore store;
    private final RecordingRecordProcessor.Behavior behavior;

    public RecordingRecordProcessorFactory(RecordingStore store,
                                           RecordingRecordProcessor.Behavior behavior) {
        this.store = store;
        this.behavior = behavior;
    }

    @Override
    public IRecordProcessor createProcessor() {
        return new RecordingRecordProcessor(store, behavior);
    }
}
