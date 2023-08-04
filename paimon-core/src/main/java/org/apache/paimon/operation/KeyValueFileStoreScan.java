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
import org.apache.paimon.io.DataFileMeta;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.operation.FileStoreScan.Plan.groupByPartFiles;

/** {@link FileStoreScan} for {@link KeyValueFileStore}. */
public class KeyValueFileStoreScan extends AbstractFileStoreScan {

    private final FieldStatsConverters fieldStatsConverters;

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
            Integer scanManifestParallelism) {
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
        this.fieldStatsConverters =
                new FieldStatsConverters(
                        sid -> keyValueFieldsExtractor.keyFields(scanTableSchema(sid)), schemaId);
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
                                        fieldStatsConverters.getOrCreate(entry.file().schemaId()),
                                        entry.file().rowCount()));
    }

    @Override
    protected List<ManifestEntry> filterByStats(List<ManifestEntry> entries) {
        if(valueFilter == null) {
            return entries;
        }

        Map<BinaryRow, Map<Integer, List<DataFileMeta>>> groupByPartFiles = groupByPartFiles(entries);
        groupByPartFiles.values().stream().flatMap(m -> m.values().stream())
                .map(this::filterHighestLevel)
    }

    private List<List<DataFileMeta>> splitByBucket(List<ManifestEntry> entries) {
        Map<BinaryRow, Map<Integer, List<DataFileMeta>>> groupBy = new HashMap<>();
        for (ManifestEntry entry : entries) {
            groupBy.computeIfAbsent(entry.partition(), k -> new HashMap<>())
                    .computeIfAbsent(entry.bucket(), k -> new ArrayList<>())
                    .add(entry.file());
        }
        return groupBy;
    }

    private List<ManifestEntry> filterHighestLevel(List<ManifestEntry> entries) {
        List<ManifestEntry> result = new ArrayList<>();

        int maxLevel = entries.stream()
                .mapToInt(e -> e.file().level()).max().orElse(-1);
        for (ManifestEntry entry: entries) {
            if (entry.file().level() == maxLevel) {
                if (valueFilter.test(
                        entry.file().rowCount(),
                        entry.file()
                                .valueStats()
                                .fields(
                                        fieldStatsConverters.getOrCreate(entry.file().schemaId()),
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
