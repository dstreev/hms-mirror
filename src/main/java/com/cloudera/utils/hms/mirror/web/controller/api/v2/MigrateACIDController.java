/*
 * Copyright (c) 2025. Cloudera, Inc. All Rights Reserved
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


package com.cloudera.utils.hms.mirror.web.controller.api.v2;

import com.cloudera.utils.hms.mirror.domain.*; // Import domain object
import com.cloudera.utils.hms.mirror.web.controller.api.v2.service.ConfigurationService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * REST Controller for managing the 'migrateACID' section of the HmsMirror configuration.
 * Provides endpoints to retrieve and update ACID migration settings.
 */
@RestController("MigrateACIDControllerV2")
@RequestMapping("/api/v2/config/migrateACID")
@Tag(name = "Migrate ACID Configuration", description = "Operations related to ACID table migration properties.")
@Slf4j
public class MigrateACIDController {

    @Autowired
    private ConfigurationService configService;

    /**
     * Retrieves the current MigrateACID configuration.
     * @return A ResponseEntity containing the MigrateACID domain object.
     */
    @GetMapping
    @Operation(summary = "Get current Migrate ACID configuration",
            description = "Retrieves the complete current Migrate ACID configuration properties.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully retrieved Migrate ACID configuration"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<MigrateACID> getMigrateACIDConfig() {
        log.info("Received request to get Migrate ACID configuration.");
        MigrateACID migrateACID = configService.getConfiguration().getMigrateACID();
        log.info("Returning Migrate ACID configuration: {}", migrateACID);
        return ResponseEntity.ok(migrateACID);
    }

    /**
     * Updates the MigrateACID configuration.
     * Only provided (non-null) fields in the request body will update the existing configuration.
     * @param migrateACID A partial MigrateACID domain object with fields to update.
     * @return A ResponseEntity containing the updated MigrateACID domain object.
     */
    @PutMapping
    @Operation(summary = "Update Migrate ACID configuration",
            description = "Updates specific properties within the Migrate ACID configuration. " +
                    "Only provided (non-null) fields will be updated.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully updated Migrate ACID configuration"),
            @ApiResponse(responseCode = "400", description = "Invalid request body"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<MigrateACID> updateMigrateACIDConfig(@RequestBody MigrateACID migrateACID) {
        log.info("Received request to update Migrate ACID configuration with data: {}", migrateACID);
        HmsMirrorConfig currentHmsMirrorConfig = configService.getConfiguration();
        if (currentHmsMirrorConfig.getMigrateACID() == null) {
            currentHmsMirrorConfig.setMigrateACID(new MigrateACID());
            log.debug("MigrateACID object was null, initialized a new one.");
        }
        configService.updateMigrateACID(currentHmsMirrorConfig.getMigrateACID(), migrateACID);
        log.info("Migrate ACID configuration updated successfully.");
        return ResponseEntity.ok(currentHmsMirrorConfig.getMigrateACID());
    }
}
