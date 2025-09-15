/*
 * Copyright (c) 2024-2025. Cloudera, Inc. All Rights Reserved
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.cloudera.utils.hms.mirror.core.model;

/**
 * Configuration for optimization settings and calculations.
 */
public class OptimizationConfiguration {
    private final boolean autoTune;
    private final boolean skipStatsCollection;
    private final boolean enableCompression;
    private final int defaultReducerCount;
    private final long defaultTargetFileSize;

    public OptimizationConfiguration(boolean autoTune, boolean skipStatsCollection, 
                                   boolean enableCompression, int defaultReducerCount,
                                   long defaultTargetFileSize) {
        this.autoTune = autoTune;
        this.skipStatsCollection = skipStatsCollection;
        this.enableCompression = enableCompression;
        this.defaultReducerCount = defaultReducerCount;
        this.defaultTargetFileSize = defaultTargetFileSize;
    }

    public boolean isAutoTune() { return autoTune; }
    public boolean isSkipStatsCollection() { return skipStatsCollection; }
    public boolean isEnableCompression() { return enableCompression; }
    public int getDefaultReducerCount() { return defaultReducerCount; }
    public long getDefaultTargetFileSize() { return defaultTargetFileSize; }
}