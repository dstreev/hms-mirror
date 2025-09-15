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
 * Result of partition distribution ratio calculation.
 */
public class PartitionDistributionResult {
    private final boolean success;
    private final Long distributionRatio;
    private final Long avgPartitionSize;
    private final String message;

    public PartitionDistributionResult(boolean success, Long distributionRatio, 
                                     Long avgPartitionSize, String message) {
        this.success = success;
        this.distributionRatio = distributionRatio;
        this.avgPartitionSize = avgPartitionSize;
        this.message = message;
    }

    public static PartitionDistributionResult success(Long distributionRatio, Long avgPartitionSize) {
        return new PartitionDistributionResult(true, distributionRatio, avgPartitionSize, 
                                             "Distribution ratio calculated successfully");
    }

    public static PartitionDistributionResult failure(String message) {
        return new PartitionDistributionResult(false, null, null, message);
    }

    public boolean isSuccess() { return success; }
    public Long getDistributionRatio() { return distributionRatio; }
    public Long getAvgPartitionSize() { return avgPartitionSize; }
    public String getMessage() { return message; }
}