/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.listener;

import org.apache.paimon.Snapshot;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.utils.FileStorePathFactory;

import java.util.List;
import java.util.Map;

/**
 * aaa.
 */
public class CommitEvents {

    private Listeners listeners;

    private FileStorePathFactory pathFactory;

    public CommitEvents(Listeners listeners, FileStorePathFactory pathFactory) {
        this.listeners = listeners;
        this.pathFactory = pathFactory;
    }

    public void commitEvent(List<ManifestEntry> tableFiles,
                            List<ManifestEntry> changelogFiles,
                            List<IndexManifestEntry> indexFiles,
                            long identifier,
                            Long watermark,
                            Map<Integer, Long> logOffsets,
                            Snapshot.CommitKind commitKind,
                            Long safeLatestSnapshotId,
                            Long time,
                            String failReason,
                            Event.State state) {
        listeners.notify(new CommitEvent(
                pathFactory.root().toString(),
                tableFiles,
                changelogFiles,
                indexFiles,
                identifier,
                watermark,
                logOffsets,
                commitKind,
                safeLatestSnapshotId,
                time,
                failReason,
                state
        ));
    }
}
