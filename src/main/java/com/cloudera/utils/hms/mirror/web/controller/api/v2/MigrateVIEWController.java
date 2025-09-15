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
 * REST Controller for managing the 'migrateVIEW' section of the HmsMirror configuration.
 * Provides endpoints to retrieve and update VIEW migration settings.
 */
@RestController("MigrateVIEWControllerV2")
@RequestMapping("/api/v2/config/migrateVIEW")
@Tag(name = "Migrate VIEW Configuration", description = "Operations related to VIEW migration properties.")
@Slf4j
public class MigrateVIEWController {

    @Autowired
    private ConfigurationService configService;

    /**
     * Retrieves the current MigrateVIEW configuration.
     * @return A ResponseEntity containing the MigrateVIEW domain object.
     */
    @GetMapping
    @Operation(summary = "Get current Migrate VIEW configuration",
            description = "Retrieves the complete current Migrate VIEW configuration properties.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully retrieved Migrate VIEW configuration"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<MigrateVIEW> getMigrateVIEWConfig() {
        log.info("Received request to get Migrate VIEW configuration.");
        MigrateVIEW migrateVIEW = configService.getConfiguration().getMigrateVIEW();
        log.info("Returning Migrate VIEW configuration: {}", migrateVIEW);
        return ResponseEntity.ok(migrateVIEW);
    }

    /**
     * Updates the MigrateVIEW configuration.
     * Only provided (non-null) fields in the request body will update the existing configuration.
     * @param migrateVIEW A partial MigrateVIEW domain object with fields to update.
     * @return A ResponseEntity containing the updated MigrateVIEW domain object.
     */
    @PutMapping
    @Operation(summary = "Update Migrate VIEW configuration",
            description = "Updates specific properties within the Migrate VIEW configuration. " +
                    "Only provided (non-null) fields will be updated.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully updated Migrate VIEW configuration"),
            @ApiResponse(responseCode = "400", description = "Invalid request body"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<MigrateVIEW> updateMigrateVIEWConfig(@RequestBody MigrateVIEW migrateVIEW) {
        log.info("Received request to update Migrate VIEW configuration with data: {}", migrateVIEW);
        HmsMirrorConfig currentHmsMirrorConfig = configService.getConfiguration();
        if (currentHmsMirrorConfig.getMigrateVIEW() == null) {
            currentHmsMirrorConfig.setMigrateVIEW(new MigrateVIEW());
            log.debug("MigrateVIEW object was null, initialized a new one.");
        }
        configService.updateMigrateVIEW(currentHmsMirrorConfig.getMigrateVIEW(), migrateVIEW);
        log.info("Migrate VIEW configuration updated successfully.");
        return ResponseEntity.ok(currentHmsMirrorConfig.getMigrateVIEW());
    }
}
