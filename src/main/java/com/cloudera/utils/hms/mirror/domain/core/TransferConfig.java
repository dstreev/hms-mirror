/*
 * Copyright (c) 2023-2025. Cloudera, Inc. All Rights Reserved
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

package com.cloudera.utils.hms.mirror.domain.core;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

//@Component("transfer")
@Slf4j
@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
@Schema(description = "Data transfer configuration for migrating tables between clusters, " +
        "including prefixes, directories, and storage migration settings")
public class TransferConfig implements Cloneable {
    @Deprecated // Moved to Application Configuration *application.yml*
//    private int concurrency = 4;
    @Schema(description = "Prefix for intermediate transfer tables created during migration",
            defaultValue = "hms_mirror_transfer_",
            example = "hms_mirror_transfer_")
    private String transferPrefix = "hms_mirror_transfer_";

    @Schema(description = "Prefix for shadow tables used to validate migration before final commit",
            defaultValue = "hms_mirror_shadow_",
            example = "hms_mirror_shadow_")
    private String shadowPrefix = "hms_mirror_shadow_";

    @Schema(description = "Postfix appended to table names during storage migration operations",
            defaultValue = "_storage_migration",
            example = "_storage_migration")
    private String storageMigrationPostfix = "_storage_migration";

    @Schema(description = "Base directory prefix for EXPORT operations in Hive EXPORT/IMPORT migration strategy",
            defaultValue = "/apps/hive/warehouse/export_",
            example = "/apps/hive/warehouse/export_")
    private String exportBaseDirPrefix = "/apps/hive/warehouse/export_";

    @Schema(description = "Remote working directory for temporary files during cross-cluster migration",
            defaultValue = "hms_mirror_working",
            example = "hms_mirror_working")
    private String remoteWorkingDirectory = "hms_mirror_working";

    @Deprecated // Moved to Job.
    @Schema(description = "Target namespace for storage migration (deprecated - moved to Job configuration)",
            deprecated = true)
    private String targetNamespace = null;

    @Schema(description = "Intermediate storage location for temporary data during multi-stage migrations",
            example = "s3a://temp-bucket/migration/intermediate/")
    private String intermediateStorage = null;

    @Schema(description = "Storage migration configuration defining translation type, data movement strategy, and flow direction")
    private StorageMigration storageMigration = null;

    @Schema(description = "Warehouse directory configuration for external and managed tables on the target cluster")
    private Warehouse warehouse = null;

    @Override
    public TransferConfig clone() {
        try {
            TransferConfig clone = (TransferConfig) super.clone();
            // Deep copy mutable objects to ensure clone independence
            if (nonNull(storageMigration)) {
                clone.storageMigration = storageMigration.clone();
            }
            if (nonNull(warehouse)) {
                clone.warehouse = warehouse.clone();
            }
            // All other fields are Strings (immutable) - no additional copying needed
            return clone;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }

    public StorageMigration getStorageMigration() {
        if (isNull(storageMigration))
            storageMigration = new StorageMigration();
        return storageMigration;
    }

}
