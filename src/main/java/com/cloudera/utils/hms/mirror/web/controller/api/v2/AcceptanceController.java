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

import com.cloudera.utils.hms.mirror.domain.Acceptance; // Import domain object
import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig; // Import top-level domain object
import com.cloudera.utils.hms.mirror.web.controller.api.v2.service.ConfigurationService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.*;

/**
 * REST Controller for managing the 'acceptance' section of the HmsMirror configuration.
 * Provides endpoints to retrieve and update acceptance-related settings.
 */
@RestController("AcceptanceControllerV2")
@RequestMapping("/api/v2/config/acceptance")
@Tag(name = "Acceptance Configuration", description = "Operations related to the acceptance configuration properties.")
@Slf4j
public class AcceptanceController {

    @Autowired
    private ConfigurationService configService;

    /**
     * Retrieves the current acceptance configuration.
     * @return A ResponseEntity containing the Acceptance domain object.
     */
    @GetMapping
    @Operation(summary = "Get current acceptance configuration",
            description = "Retrieves the complete current acceptance configuration properties.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully retrieved acceptance configuration"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<Acceptance> getAcceptanceConfig() {
        log.info("Received request to get acceptance configuration.");
        Acceptance acceptance = configService.getConfiguration().getAcceptance();
        log.info("Returning acceptance configuration: {}", acceptance);
        return ResponseEntity.ok(acceptance);
    }

    /**
     * Updates the acceptance configuration.
     * Only provided (non-null) fields in the request body will update the existing configuration.
     * @param acceptance A partial Acceptance domain object with fields to update.
     * @return A ResponseEntity containing the updated Acceptance domain object.
     */
    @PutMapping
    @Operation(summary = "Update acceptance configuration",
            description = "Updates specific properties within the acceptance configuration. " +
                    "Only provided (non-null) fields will be updated.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully updated acceptance configuration"),
            @ApiResponse(responseCode = "400", description = "Invalid request body"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<Acceptance> updateAcceptanceConfig(@RequestBody Acceptance acceptance) {
        log.info("Received request to update acceptance configuration with data: {}", acceptance);
        HmsMirrorConfig currentHmsMirrorConfig = configService.getConfiguration();
        // The init() method ensures acceptance is never null, but an extra check for safety doesn't hurt.
        if (currentHmsMirrorConfig.getAcceptance() == null) {
            currentHmsMirrorConfig.setAcceptance(new Acceptance());
            log.debug("Acceptance object was null, initialized a new one.");
        }
        configService.updateAcceptance(currentHmsMirrorConfig.getAcceptance(), acceptance);
        log.info("Acceptance configuration updated successfully.");
        return ResponseEntity.ok(currentHmsMirrorConfig.getAcceptance());
    }
}
