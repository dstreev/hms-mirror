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

import com.cloudera.utils.hms.mirror.domain.support.SerdeType;

/**
 * Result of Tez max grouping calculation.
 */
public class TezGroupingCalculationResult {
    private final boolean success;
    private final Long maxGroupingSize;
    private final SerdeType serdeType;
    private final String reason;

    public TezGroupingCalculationResult(boolean success, Long maxGroupingSize, 
                                      SerdeType serdeType, String reason) {
        this.success = success;
        this.maxGroupingSize = maxGroupingSize;
        this.serdeType = serdeType;
        this.reason = reason;
    }

    public static TezGroupingCalculationResult success(Long maxGroupingSize, SerdeType serdeType) {
        return new TezGroupingCalculationResult(true, maxGroupingSize, serdeType, 
                                              "Tez grouping calculated successfully");
    }

    public static TezGroupingCalculationResult failure(String reason) {
        return new TezGroupingCalculationResult(false, null, null, reason);
    }

    public boolean isSuccess() { return success; }
    public Long getMaxGroupingSize() { return maxGroupingSize; }
    public SerdeType getSerdeType() { return serdeType; }
    public String getReason() { return reason; }
}