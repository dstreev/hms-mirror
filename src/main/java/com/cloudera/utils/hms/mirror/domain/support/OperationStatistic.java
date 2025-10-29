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

import java.util.concurrent.atomic.AtomicInteger;

@Getter
public class OperationStatistic implements Cloneable {
    private final AtomicInteger databases = new AtomicInteger(0);
    private final AtomicInteger tables = new AtomicInteger(0);

    public int incrementDatabases() {
        return databases.incrementAndGet();
    }

    public int incrementTables() {
        return tables.incrementAndGet();
    }

    public void reset() {
        databases.set(0);
        tables.set(0);
    }

    @Override
    public OperationStatistic clone() {
        try {
            OperationStatistic clone = (OperationStatistic) super.clone();
            // Note: AtomicInteger fields are final and cannot be reassigned.
            // However, since this is a statistics object, the shallow copy of AtomicInteger
            // references means both objects share the same counters, which is typically
            // not desired for a true clone. Consider using a copy constructor pattern instead
            // if true independence is needed.
            return clone;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }

    /**
     * Creates a new OperationStatistic with copied values from this instance.
     * This provides true independence from the original object.
     */
    public OperationStatistic deepCopy() {
        OperationStatistic copy = new OperationStatistic();
        copy.databases.set(this.databases.get());
        copy.tables.set(this.tables.get());
        return copy;
    }
}
