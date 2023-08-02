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

import java.util.List;
import java.util.Map;

/**
 * This event will be sent after the result of the Commit operation.
 */
public class CommitEvent implements Event{
    private List<ManifestEntry> tableFiles;
    private List<ManifestEntry> changelogFiles;
    private List<IndexManifestEntry> indexFiles;
    private long identifier;
    private Long watermark;
    private Map<Integer, Long> logOffsets;
    private Snapshot.CommitKind commitKind;
    private Long safeLatestSnapshotId;

    private Long time;

    private String failReason;

    private State state;

    private String path;

    public CommitEvent(String path,
                       List<ManifestEntry> tableFiles,
                       List<ManifestEntry> changelogFiles,
                       List<IndexManifestEntry> indexFiles,
                       long identifier,
                       Long watermark,
                       Map<Integer, Long> logOffsets,
                       Snapshot.CommitKind commitKind,
                       Long safeLatestSnapshotId,
                       Long time,
                       String failReason,
                       State state) {
        this.tableFiles = tableFiles;
        this.changelogFiles = changelogFiles;
        this.indexFiles = indexFiles;
        this.identifier = identifier;
        this.watermark = watermark;
        this.logOffsets = logOffsets;
        this.commitKind = commitKind;
        this.safeLatestSnapshotId = safeLatestSnapshotId;
        this.time = time;
        this.failReason = failReason;
        this.state = state;
        this.path = path;
    }

    public List<ManifestEntry> getTableFiles() {
        return tableFiles;
    }

    public void setTableFiles(List<ManifestEntry> tableFiles) {
        this.tableFiles = tableFiles;
    }

    public List<ManifestEntry> getChangelogFiles() {
        return changelogFiles;
    }

    public void setChangelogFiles(List<ManifestEntry> changelogFiles) {
        this.changelogFiles = changelogFiles;
    }

    public List<IndexManifestEntry> getIndexFiles() {
        return indexFiles;
    }

    public void setIndexFiles(List<IndexManifestEntry> indexFiles) {
        this.indexFiles = indexFiles;
    }

    public long getIdentifier() {
        return identifier;
    }

    public void setIdentifier(long identifier) {
        this.identifier = identifier;
    }

    public Long getWatermark() {
        return watermark;
    }

    public void setWatermark(Long watermark) {
        this.watermark = watermark;
    }

    public Map<Integer, Long> getLogOffsets() {
        return logOffsets;
    }

    public void setLogOffsets(Map<Integer, Long> logOffsets) {
        this.logOffsets = logOffsets;
    }

    public Snapshot.CommitKind getCommitKind() {
        return commitKind;
    }

    public void setCommitKind(Snapshot.CommitKind commitKind) {
        this.commitKind = commitKind;
    }

    public Long getSafeLatestSnapshotId() {
        return safeLatestSnapshotId;
    }

    public void setSafeLatestSnapshotId(Long safeLatestSnapshotId) {
        this.safeLatestSnapshotId = safeLatestSnapshotId;
    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    public String getFailReason() {
        return failReason;
    }

    public void setFailReason(String failReason) {
        this.failReason = failReason;
    }

    public void setState(State state) {
        this.state = state;
    }

    @Override
    public String path() {
        return path;
    }

    @Override
    public State state() {
        return state;
    }
}
