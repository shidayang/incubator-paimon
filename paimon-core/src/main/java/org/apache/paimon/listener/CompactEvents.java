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
import org.apache.paimon.utils.FileStorePathFactory;

import java.util.List;

/**
 * aa.
 */
public class CompactEvents {

    private Listeners listeners;

    private FileStorePathFactory pathFactory;

    private BinaryRow partition;

    private int bucket;

    public CompactEvents(Listeners listeners, FileStorePathFactory pathFactory) {
        this.listeners = listeners;
        this.pathFactory = pathFactory;
    }

    private CompactEvents(Listeners listeners, FileStorePathFactory pathFactory, BinaryRow partition, int bucket) {
        this.listeners = listeners;
        this.pathFactory = pathFactory;
        this.partition = partition;
        this.bucket = bucket;
    }

    public CompactEvents copyFor(BinaryRow partition, int bucket) {
        return new CompactEvents(listeners, pathFactory, partition, bucket);
    }

    public void compactEvent(Event.State state,
                             boolean fullCompaction,
                             long time,
                             List<DataFileMeta> beforeFiles,
                             List<DataFileMeta> afterFiles,
                             String failReason) {
        listeners.notify(new CompactEvent(
                pathFactory.root().toString(),
                state,
                fullCompaction,
                time,
                partition,
                bucket,
                beforeFiles,
                afterFiles,
                failReason));
    }
}
