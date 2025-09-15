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

import com.cloudera.utils.hms.mirror.datastrategy.DataStrategy;
import com.cloudera.utils.hms.mirror.domain.support.DataStrategyEnum;

import java.util.List;

/**
 * Result of data strategy selection.
 */
public class DataStrategySelectionResult {
    private final boolean success;
    private final DataStrategy selectedStrategy;
    private final DataStrategyEnum strategyType;
    private final String reason;
    private final List<String> warnings;

    public DataStrategySelectionResult(boolean success, DataStrategy selectedStrategy,
                                     DataStrategyEnum strategyType, String reason,
                                     List<String> warnings) {
        this.success = success;
        this.selectedStrategy = selectedStrategy;
        this.strategyType = strategyType;
        this.reason = reason;
        this.warnings = warnings;
    }

    public static DataStrategySelectionResult success(DataStrategy strategy, DataStrategyEnum strategyType, 
                                                    String reason) {
        return new DataStrategySelectionResult(true, strategy, strategyType, reason, List.of());
    }

    public static DataStrategySelectionResult success(DataStrategy strategy, DataStrategyEnum strategyType, 
                                                    String reason, List<String> warnings) {
        return new DataStrategySelectionResult(true, strategy, strategyType, reason, warnings);
    }

    public static DataStrategySelectionResult failure(String reason) {
        return new DataStrategySelectionResult(false, null, null, reason, List.of());
    }

    public boolean isSuccess() { return success; }
    public DataStrategy getSelectedStrategy() { return selectedStrategy; }
    public DataStrategyEnum getStrategyType() { return strategyType; }
    public String getReason() { return reason; }
    public List<String> getWarnings() { return warnings; }
}