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
 * REST Controller for managing the 'hybrid' section of the HmsMirror configuration.
 * Provides endpoints to retrieve and update hybrid strategy settings.
 */
@RestController("HybridControllerV2")
@RequestMapping("/api/v2/config/hybrid")
@Tag(name = "Hybrid Configuration", description = "Operations related to hybrid data movement strategy properties.")
@Slf4j
public class HybridController {

    @Autowired
    private ConfigurationService configService;

    /**
     * Retrieves the current hybrid configuration.
     * @return A ResponseEntity containing the HybridConfig domain object.
     */
    @GetMapping
    @Operation(summary = "Get current hybrid configuration",
            description = "Retrieves the complete current hybrid configuration properties.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully retrieved hybrid configuration"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<HybridConfig> getHybridConfig() {
        log.info("Received request to get hybrid configuration.");
        HybridConfig hybridConfig = configService.getConfiguration().getHybrid();
        log.info("Returning hybrid configuration: {}", hybridConfig);
        return ResponseEntity.ok(hybridConfig);
    }

    /**
     * Updates the hybrid configuration.
     * Only provided (non-null) fields in the request body will update the existing configuration.
     * @param hybridConfig A partial HybridConfig domain object with fields to update.
     * @return A ResponseEntity containing the updated HybridConfig domain object.
     */
    @PutMapping
    @Operation(summary = "Update hybrid configuration",
            description = "Updates specific properties within the hybrid configuration. " +
                    "Only provided (non-null) fields will be updated.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully updated hybrid configuration"),
            @ApiResponse(responseCode = "400", description = "Invalid request body"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<HybridConfig> updateHybridConfig(@RequestBody HybridConfig hybridConfig) {
        log.info("Received request to update hybrid configuration with data: {}", hybridConfig);
        HmsMirrorConfig currentHmsMirrorConfig = configService.getConfiguration();
        if (currentHmsMirrorConfig.getHybrid() == null) {
            currentHmsMirrorConfig.setHybrid(new HybridConfig());
            log.debug("HybridConfig object was null, initialized a new one.");
        }
        configService.updateHybridConfig(currentHmsMirrorConfig.getHybrid(), hybridConfig);
        log.info("Hybrid configuration updated successfully.");
        return ResponseEntity.ok(currentHmsMirrorConfig.getHybrid());
    }
}
