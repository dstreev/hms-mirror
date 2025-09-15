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
 * Result of auto stats collection settings calculation.
 */
public class AutoStatsResult {
    private final boolean enableTableStats;
    private final boolean enableColumnStats;
    private final String reason;

    public AutoStatsResult(boolean enableTableStats, boolean enableColumnStats, String reason) {
        this.enableTableStats = enableTableStats;
        this.enableColumnStats = enableColumnStats;
        this.reason = reason;
    }

    public static AutoStatsResult enabled(boolean tableStats, boolean columnStats, String reason) {
        return new AutoStatsResult(tableStats, columnStats, reason);
    }

    public static AutoStatsResult disabled(String reason) {
        return new AutoStatsResult(false, false, reason);
    }

    public boolean isEnableTableStats() { return enableTableStats; }
    public boolean isEnableColumnStats() { return enableColumnStats; }
    public String getReason() { return reason; }
}