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

package org.apache.paimon.operation;

import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.KeyValueFieldsExtractor;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.stats.FieldStatsConverters;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.SnapshotManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** {@link FileStoreScan} for {@link KeyValueFileStore}. */
public class KeyValueFileStoreScan extends AbstractFileStoreScan {

    private final FieldStatsConverters keyFieldStatsConverters;

    private final FieldStatsConverters valueFieldStatsConverters;

    private final boolean hasSequenceField;

    private Predicate keyFilter;

    private Predicate valueFilter;

    public KeyValueFileStoreScan(
            RowType partitionType,
            ScanBucketFilter bucketFilter,
            SnapshotManager snapshotManager,
            SchemaManager schemaManager,
            long schemaId,
            KeyValueFieldsExtractor keyValueFieldsExtractor,
            ManifestFile.Factory manifestFileFactory,
            ManifestList.Factory manifestListFactory,
            int numOfBuckets,
            boolean checkNumOfBuckets,
            Integer scanManifestParallelism,
            boolean hasSequenceField) {
        super(
                partitionType,
                bucketFilter,
                snapshotManager,
                schemaManager,
                manifestFileFactory,
                manifestListFactory,
                numOfBuckets,
                checkNumOfBuckets,
                scanManifestParallelism);
        keyFieldStatsConverters =
                new FieldStatsConverters(
                        sid -> keyValueFieldsExtractor.keyFields(scanTableSchema(sid)), schemaId);
        valueFieldStatsConverters =
                new FieldStatsConverters(
                        sid -> keyValueFieldsExtractor.valueFields(scanTableSchema(sid)), schemaId);
        this.hasSequenceField = hasSequenceField;
    }

    public KeyValueFileStoreScan withKeyFilter(Predicate predicate) {
        this.keyFilter = predicate;
        this.bucketFilter.pushdown(predicate);
        return this;
    }

    public KeyValueFileStoreScan withValueFilter(Predicate predicate) {
        this.valueFilter = predicate;
        return this;
    }

    /** Note: Keep this thread-safe. */
    @Override
    protected boolean filterByStats(ManifestEntry entry) {
        return keyFilter == null
                || keyFilter.test(
                        entry.file().rowCount(),
                        entry.file()
                                .keyStats()
                                .fields(
                                        keyFieldStatsConverters.getOrCreate(
                                                entry.file().schemaId()),
                                        entry.file().rowCount()));
    }

    @Override
    protected List<ManifestEntry> finalFilterByStats(List<ManifestEntry> entries) {
        if (valueFilter == null || hasSequenceField) {
            return entries;
        }

        List<List<ManifestEntry>> groupByBucketFiles = splitByBucket(entries);
        return groupByBucketFiles.stream()
                .map(this::filterMaxLevel)
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }

    private List<List<ManifestEntry>> splitByBucket(List<ManifestEntry> entries) {
        Map<BinaryRow, Map<Integer, List<ManifestEntry>>> groupBy = new HashMap<>();
        for (ManifestEntry entry : entries) {
            groupBy.computeIfAbsent(entry.partition(), k -> new HashMap<>())
                    .computeIfAbsent(entry.bucket(), k -> new ArrayList<>())
                    .add(entry);
        }
        return groupBy.values().stream()
                .flatMap(bucketMap -> bucketMap.values().stream())
                .collect(Collectors.toList());
    }

    private List<ManifestEntry> filterMaxLevel(List<ManifestEntry> entries) {
        List<ManifestEntry> result = new ArrayList<>();

        int maxLevel = entries.stream().mapToInt(e -> e.file().level()).max().orElse(-1);
        for (ManifestEntry entry : entries) {
            if (entry.file().level() == maxLevel) {
                if (valueFilter.test(
                        entry.file().rowCount(),
                        entry.file()
                                .valueStats()
                                .fields(
                                        valueFieldStatsConverters.getOrCreate(
                                                entry.file().schemaId()),
                                        entry.file().rowCount()))) {
                    result.add(entry);
                }
            } else {
                result.add(entry);
            }
        }
        return result;
    }
}
