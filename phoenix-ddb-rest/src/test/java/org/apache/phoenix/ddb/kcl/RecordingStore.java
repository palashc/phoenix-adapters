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

import com.amazonaws.services.dynamodbv2.model.Record;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Thread-safe in-memory sink used by {@link RecordingRecordProcessor}. Tracks records in
 * global arrival order across all shards and per-shard arrival order, enabling tests to
 * assert cross-shard ordering invariants (e.g. parent-before-daughter) and per-shard
 * delivery. All public reads return snapshots.
 */
public class RecordingStore {

    private final AtomicInteger arrivalCounter = new AtomicInteger();
    private final List<Entry> allEntries = new CopyOnWriteArrayList<>();
    private final Map<String, List<Entry>> entriesByShard = new ConcurrentHashMap<>();

    public void append(String shardId, Record record) {
        Entry entry = new Entry(arrivalCounter.getAndIncrement(), shardId, record);
        allEntries.add(entry);
        entriesByShard.computeIfAbsent(shardId, k -> new CopyOnWriteArrayList<>()).add(entry);
    }

    public int totalCount() {
        return allEntries.size();
    }

    public List<Record> allRecordsInArrival() {
        return allEntries.stream().map(e -> e.record).collect(Collectors.toList());
    }

    public Set<String> shardsSeen() {
        return new java.util.HashSet<>(entriesByShard.keySet());
    }

    /** Global arrival index of the first record on {@code shardId}, or -1 if none yet. */
    public int firstArrivalIndexForShard(String shardId) {
        List<Entry> list = entriesByShard.get(shardId);
        if (list == null || list.isEmpty()) {
            return -1;
        }
        return list.get(0).arrivalIndex;
    }

    /** Global arrival index of the last record on {@code shardId}, or -1 if none yet. */
    public int lastArrivalIndexForShard(String shardId) {
        List<Entry> list = entriesByShard.get(shardId);
        if (list == null || list.isEmpty()) {
            return -1;
        }
        return list.get(list.size() - 1).arrivalIndex;
    }

    /** {@code shardId -> count} snapshot for diagnostic logging. */
    public Map<String, Integer> countByShard() {
        Map<String, Integer> counts = new LinkedHashMap<>();
        for (Map.Entry<String, List<Entry>> e : entriesByShard.entrySet()) {
            counts.put(e.getKey(), e.getValue().size());
        }
        return counts;
    }

    private static final class Entry {
        final int arrivalIndex;
        final String shardId;
        final Record record;

        Entry(int arrivalIndex, String shardId, Record record) {
            this.arrivalIndex = arrivalIndex;
            this.shardId = shardId;
            this.record = record;
        }
    }
}
