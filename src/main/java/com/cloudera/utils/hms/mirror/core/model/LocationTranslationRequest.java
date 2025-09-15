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

import com.cloudera.utils.hms.mirror.domain.TableMirror;

/**
 * Request object for location translation operations.
 * Contains all necessary information to perform location translation business logic.
 */
public class LocationTranslationRequest {
    private final TableMirror tableMirror;
    private final String originalLocation;
    private final int level;
    private final String partitionSpec;

    public LocationTranslationRequest(TableMirror tableMirror, String originalLocation, 
                                    int level, String partitionSpec) {
        this.tableMirror = tableMirror;
        this.originalLocation = originalLocation;
        this.level = level;
        this.partitionSpec = partitionSpec;
    }

    public TableMirror getTableMirror() { return tableMirror; }
    public String getOriginalLocation() { return originalLocation; }
    public int getLevel() { return level; }
    public String getPartitionSpec() { return partitionSpec; }
}