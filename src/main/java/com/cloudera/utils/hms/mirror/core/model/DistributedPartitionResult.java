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
 * Result of distributed partition elements generation.
 */
public class DistributedPartitionResult {
    private final boolean success;
    private final String partitionElements;
    private final Long distributionRatio;
    private final String message;

    public DistributedPartitionResult(boolean success, String partitionElements, 
                                    Long distributionRatio, String message) {
        this.success = success;
        this.partitionElements = partitionElements;
        this.distributionRatio = distributionRatio;
        this.message = message;
    }

    public static DistributedPartitionResult success(String partitionElements, Long distributionRatio) {
        return new DistributedPartitionResult(true, partitionElements, distributionRatio, 
                                            "Partition elements generated successfully");
    }

    public static DistributedPartitionResult failure(String message) {
        return new DistributedPartitionResult(false, "", null, message);
    }

    public boolean isSuccess() { return success; }
    public String getPartitionElements() { return partitionElements; }
    public Long getDistributionRatio() { return distributionRatio; }
    public String getMessage() { return message; }
}