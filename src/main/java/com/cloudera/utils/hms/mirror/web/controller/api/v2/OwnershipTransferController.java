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
 * REST Controller for managing the 'ownershipTransfer' section of the HmsMirror configuration.
 * Provides endpoints to retrieve and update ownership transfer settings.
 */
@RestController("OwnershipTransferControllerV2")
@RequestMapping("/api/v2/config/ownershipTransfer")
@Tag(name = "Ownership Transfer Configuration", description = "Operations related to database and table ownership transfer properties.")
@Slf4j
public class OwnershipTransferController {

    @Autowired
    private ConfigurationService configService;

    /**
     * Retrieves the current ownership transfer configuration.
     * @return A ResponseEntity containing the TransferOwnership domain object.
     */
    @GetMapping
    @Operation(summary = "Get current ownership transfer configuration",
            description = "Retrieves the complete current ownership transfer configuration properties.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully retrieved ownership transfer configuration"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<TransferOwnership> getOwnershipTransferConfig() {
        log.info("Received request to get ownership transfer configuration.");
        TransferOwnership ownershipTransfer = configService.getConfiguration().getOwnershipTransfer();
        log.info("Returning ownership transfer configuration: {}", ownershipTransfer);
        return ResponseEntity.ok(ownershipTransfer);
    }

    /**
     * Updates the ownership transfer configuration.
     * Only provided (non-null) fields in the request body will update the existing configuration.
     * @param ownershipTransfer A partial TransferOwnership domain object with fields to update.
     * @return A ResponseEntity containing the updated TransferOwnership domain object.
     */
    @PutMapping
    @Operation(summary = "Update ownership transfer configuration",
            description = "Updates specific properties within the ownership transfer configuration. " +
                    "Only provided (non-null) fields will be updated.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully updated ownership transfer configuration"),
            @ApiResponse(responseCode = "400", description = "Invalid request body"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<TransferOwnership> updateOwnershipTransferConfig(@RequestBody TransferOwnership ownershipTransfer) {
        log.info("Received request to update ownership transfer configuration with data: {}", ownershipTransfer);
        HmsMirrorConfig currentHmsMirrorConfig = configService.getConfiguration();
        if (currentHmsMirrorConfig.getOwnershipTransfer() == null) {
            currentHmsMirrorConfig.setOwnershipTransfer(new TransferOwnership());
            log.debug("TransferOwnership object was null, initialized a new one.");
        }
        configService.updateTransferOwnership(currentHmsMirrorConfig.getOwnershipTransfer(), ownershipTransfer);
        log.info("Ownership transfer configuration updated successfully.");
        return ResponseEntity.ok(currentHmsMirrorConfig.getOwnershipTransfer());
    }
}
