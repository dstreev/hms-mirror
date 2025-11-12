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

import io.swagger.v3.oas.annotations.media.Schema;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.RocksDB;

import java.util.Arrays;
import java.util.List;

/**
 * Centralized enumeration of all RocksDB column families used by HMS-Mirror.
 * This ensures consistency across the application and provides a single source
 * of truth for column family names and descriptors.
 */
@Schema(description = "RocksDB column family identifiers")
public enum RocksDBColumnFamily {
    
    @Schema(description = "Default column family (RocksDB built-in)")
    DEFAULT("default", RocksDB.DEFAULT_COLUMN_FAMILY),
    
    @Schema(description = "HMS-Mirror configuration storage")
    CONFIGURATIONS("configurations", "configurations".getBytes()),
    
    @Schema(description = "Session data storage")
    SESSIONS("sessions", "sessions".getBytes()),
    
    @Schema(description = "Connection configuration storage")
    CONNECTIONS("connections", "connections".getBytes()),
    
    @Schema(description = "Dataset configuration storage")
    DATASETS("datasets", "datasets".getBytes()),

    @Schema(description = "Job configuration storage")
    JOBS("jobs", "jobs".getBytes()),

    @Schema(description = "Conversion result storage")
    CONVERSION_RESULT("conversionResult", "conversionResult".getBytes()),

    @Schema(description = "RunStatus result storage")
    RUN_STATUS("runStatus", "runStatus".getBytes());

    private final String displayName;
    private final byte[] keyBytes;
    
    RocksDBColumnFamily(String displayName, byte[] keyBytes) {
        this.displayName = displayName;
        this.keyBytes = keyBytes;
    }
    
    /**
     * Get the display name for UI purposes
     */
    public String getDisplayName() {
        return displayName;
    }
    
    /**
     * Get the byte array representation for RocksDB operations
     */
    public byte[] getKeyBytes() {
        return keyBytes;
    }
    
    /**
     * Create a ColumnFamilyDescriptor for this column family
     */
    public ColumnFamilyDescriptor getDescriptor() {
        return new ColumnFamilyDescriptor(keyBytes);
    }
    
    /**
     * Get all column family descriptors for RocksDB initialization
     */
    public static List<ColumnFamilyDescriptor> getAllDescriptors() {
        return Arrays.stream(values())
                .map(RocksDBColumnFamily::getDescriptor)
                .toList();
    }
    
    /**
     * Get all column families as a list (for UI dropdowns, etc.)
     */
    public static List<RocksDBColumnFamily> getAllColumnFamilies() {
        return Arrays.asList(values());
    }
    
    /**
     * Find a column family by its display name
     */
    public static RocksDBColumnFamily fromDisplayName(String displayName) {
        return Arrays.stream(values())
                .filter(cf -> cf.displayName.equals(displayName))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown column family: " + displayName));
    }
    
    /**
     * Get all display names for UI purposes
     */
    public static List<String> getAllDisplayNames() {
        return Arrays.stream(values())
                .map(RocksDBColumnFamily::getDisplayName)
                .toList();
    }
}