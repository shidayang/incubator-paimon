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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;

import java.util.List;

/**
 * Events representing Compaction operations will be emitted at the beginning and end of Compaction.
 */
public class CompactEvent implements Event{

    private final String path;

    private final State state;

    private final boolean fullCompaction;

    private final long time;

    private final BinaryRow partition;

    private final int bucket;

    private final List<DataFileMeta> beforeFiles;

    private final List<DataFileMeta> afterFiles;

    private final String failReason;

    public CompactEvent(String path,
                        State state,
                        boolean fullCompaction,
                        long time,
                        BinaryRow partition,
                        int bucket,
                        List<DataFileMeta> beforeFiles,
                        List<DataFileMeta> afterFiles,
                        String failReason) {
        this.path = path;
        this.state = state;
        this.fullCompaction = fullCompaction;
        this.time = time;
        this.partition = partition;
        this.bucket = bucket;
        this.beforeFiles = beforeFiles;
        this.afterFiles = afterFiles;
        this.failReason = failReason;
    }

    public boolean isFullCompaction() {
        return fullCompaction;
    }

    public long getTime() {
        return time;
    }

    public BinaryRow getPartition() {
        return partition;
    }

    public int getBucket() {
        return bucket;
    }

    public List<DataFileMeta> getBeforeFiles() {
        return beforeFiles;
    }

    public List<DataFileMeta> getAfterFiles() {
        return afterFiles;
    }

    public String getFailReason() {
        return failReason;
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
