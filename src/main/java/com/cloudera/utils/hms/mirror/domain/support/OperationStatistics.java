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

package com.cloudera.utils.hms.mirror.domain.support;

import lombok.Getter;

@Getter
public class OperationStatistics implements Cloneable{

    private final OperationStatistic counts = new OperationStatistic();
    private final OperationStatistic skipped = new OperationStatistic();
    private final OperationStatistic issues = new OperationStatistic();
    private final OperationStatistic failures = new OperationStatistic();
    private final OperationStatistic successes = new OperationStatistic();

    @Override
    public OperationStatistics clone() {
        try {
            // Note: The OperationStatistic fields are final and share AtomicInteger references
            // after super.clone(). For true independence, create a new OperationStatistics
            // and use getters to populate independent statistics objects.
            OperationStatistics clone = new OperationStatistics();
            // Deep copy each statistic to ensure independence using getters
            clone.getCounts().getDatabases().set(this.getCounts().getDatabases().get());
            clone.getCounts().getTables().set(this.getCounts().getTables().get());
            clone.getSkipped().getDatabases().set(this.getSkipped().getDatabases().get());
            clone.getSkipped().getTables().set(this.getSkipped().getTables().get());
            clone.getIssues().getDatabases().set(this.getIssues().getDatabases().get());
            clone.getIssues().getTables().set(this.getIssues().getTables().get());
            clone.getFailures().getDatabases().set(this.getFailures().getDatabases().get());
            clone.getFailures().getTables().set(this.getFailures().getTables().get());
            clone.getSuccesses().getDatabases().set(this.getSuccesses().getDatabases().get());
            clone.getSuccesses().getTables().set(this.getSuccesses().getTables().get());
            return clone;
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    public void reset() {
        counts.reset();
        issues.reset();
        failures.reset();
        successes.reset();
    }

}
