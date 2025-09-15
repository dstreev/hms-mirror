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

import java.util.List;
import java.util.regex.Pattern;

/**
 * Filter criteria for table selection during migration operations.
 */
public class TableFilter {
    private final List<String> includeTables;
    private final List<String> excludeTables;
    private final Pattern includePattern;
    private final Pattern excludePattern;
    private final int sizeThreshold;
    private final boolean includeViews;
    private final boolean includeManagedTables;
    private final boolean includeExternalTables;

    public TableFilter(List<String> includeTables, List<String> excludeTables,
                      Pattern includePattern, Pattern excludePattern,
                      int sizeThreshold, boolean includeViews,
                      boolean includeManagedTables, boolean includeExternalTables) {
        this.includeTables = includeTables;
        this.excludeTables = excludeTables;
        this.includePattern = includePattern;
        this.excludePattern = excludePattern;
        this.sizeThreshold = sizeThreshold;
        this.includeViews = includeViews;
        this.includeManagedTables = includeManagedTables;
        this.includeExternalTables = includeExternalTables;
    }

    public static TableFilter allTables() {
        return new TableFilter(List.of(), List.of(), null, null, 
                             Integer.MAX_VALUE, true, true, true);
    }

    public List<String> getIncludeTables() { return includeTables; }
    public List<String> getExcludeTables() { return excludeTables; }
    public Pattern getIncludePattern() { return includePattern; }
    public Pattern getExcludePattern() { return excludePattern; }
    public int getSizeThreshold() { return sizeThreshold; }
    public boolean isIncludeViews() { return includeViews; }
    public boolean isIncludeManagedTables() { return includeManagedTables; }
    public boolean isIncludeExternalTables() { return includeExternalTables; }

    /**
     * Determines if the given table name matches this filter.
     */
    public boolean matches(String tableName, String tableType, long tableSize) {
        // Check explicit exclusions first
        if (excludeTables.contains(tableName)) {
            return false;
        }
        
        // Check exclude pattern
        if (excludePattern != null && excludePattern.matcher(tableName).matches()) {
            return false;
        }
        
        // Check size threshold
        if (tableSize > sizeThreshold) {
            return false;
        }
        
        // Check table type filters
        if ("VIEW".equalsIgnoreCase(tableType) && !includeViews) {
            return false;
        }
        if ("MANAGED_TABLE".equalsIgnoreCase(tableType) && !includeManagedTables) {
            return false;
        }
        if ("EXTERNAL_TABLE".equalsIgnoreCase(tableType) && !includeExternalTables) {
            return false;
        }
        
        // Check explicit inclusions
        if (!includeTables.isEmpty()) {
            return includeTables.contains(tableName);
        }
        
        // Check include pattern
        if (includePattern != null) {
            return includePattern.matcher(tableName).matches();
        }
        
        return true;
    }
}