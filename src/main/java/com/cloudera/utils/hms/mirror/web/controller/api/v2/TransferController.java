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
 * REST Controller for managing the 'transfer' section of the HmsMirror configuration.
 * Includes nested 'storageMigration' settings.
 */
@RestController("TransferControllerV2")
@RequestMapping("/api/v2/config/transfer")
@Tag(name = "Transfer Configuration", description = "Operations related to data transfer and storage migration properties.")
@Slf4j
public class TransferController {

    @Autowired
    private ConfigurationService configService;

    /**
     * Retrieves the current transfer configuration.
     * @return A ResponseEntity containing the TransferConfig domain object.
     */
    @GetMapping
    @Operation(summary = "Get current transfer configuration",
            description = "Retrieves the complete current transfer configuration properties.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully retrieved transfer configuration"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<TransferConfig> getTransferConfig() {
        log.info("Received request to get transfer configuration.");
        TransferConfig transferConfig = configService.getConfiguration().getTransfer();
        log.info("Returning transfer configuration: {}", transferConfig);
        return ResponseEntity.ok(transferConfig);
    }

    /**
     * Updates the transfer configuration.
     * Only provided (non-null) fields in the request body will update the existing configuration.
     * @param transferConfig A partial TransferConfig domain object with fields to update.
     * @return A ResponseEntity containing the updated TransferConfig domain object.
     */
    @PutMapping
    @Operation(summary = "Update transfer configuration",
            description = "Updates specific properties within the transfer configuration. " +
                    "Only provided (non-null) fields will be updated.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully updated transfer configuration"),
            @ApiResponse(responseCode = "400", description = "Invalid request body"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<TransferConfig> updateTransferConfig(@RequestBody TransferConfig transferConfig) {
        log.info("Received request to update transfer configuration with data: {}", transferConfig);
        HmsMirrorConfig currentHmsMirrorConfig = configService.getConfiguration();
        if (currentHmsMirrorConfig.getTransfer() == null) {
            currentHmsMirrorConfig.setTransfer(new TransferConfig());
            log.debug("TransferConfig object was null, initialized a new one.");
        }
        configService.updateTransferConfig(currentHmsMirrorConfig.getTransfer(), transferConfig);
        log.info("Transfer configuration updated successfully.");
        return ResponseEntity.ok(currentHmsMirrorConfig.getTransfer());
    }

    /**
     * Retrieves the current storage migration configuration within transfer settings.
     * @return A ResponseEntity containing the StorageMigration domain object.
     */
    @GetMapping("/storageMigration")
    @Operation(summary = "Get current storage migration configuration",
            description = "Retrieves the complete current storage migration configuration properties within transfer settings.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully retrieved storage migration configuration"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<StorageMigration> getStorageMigrationConfig() {
        log.info("Received request to get storage migration configuration.");
        StorageMigration storageMigration = configService.getConfiguration().getTransfer().getStorageMigration();
        log.info("Returning storage migration configuration: {}", storageMigration);
        return ResponseEntity.ok(storageMigration);
    }

    /**
     * Updates the storage migration configuration within transfer settings.
     * Only provided (non-null) fields in the request body will update the existing configuration.
     * @param storageMigration A partial StorageMigration domain object with fields to update.
     * @return A ResponseEntity containing the updated StorageMigration domain object.
     */
    @PutMapping("/storageMigration")
    @Operation(summary = "Update storage migration configuration",
            description = "Updates specific properties within the storage migration configuration. " +
                    "Only provided (non-null) fields will be updated.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully updated storage migration configuration"),
            @ApiResponse(responseCode = "400", description = "Invalid request body"),
            @ApiResponse(responseCode = "404", description = "TransferConfig object not found"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<StorageMigration> updateStorageMigrationConfig(@RequestBody StorageMigration storageMigration) {
        log.info("Received request to update storage migration configuration with data: {}", storageMigration);
        HmsMirrorConfig currentHmsMirrorConfig = configService.getConfiguration();
        if (currentHmsMirrorConfig.getTransfer() == null) {
            log.warn("TransferConfig object not found for storage migration update. Cannot proceed.");
            return ResponseEntity.notFound().build();
        }
        if (currentHmsMirrorConfig.getTransfer().getStorageMigration() == null) {
            currentHmsMirrorConfig.getTransfer().setStorageMigration(new StorageMigration());
            log.debug("StorageMigration object was null, initialized a new one.");
        }
        configService.updateStorageMigration(currentHmsMirrorConfig.getTransfer().getStorageMigration(), storageMigration);
        log.info("Storage migration configuration updated successfully.");
        return ResponseEntity.ok(currentHmsMirrorConfig.getTransfer().getStorageMigration());
    }

    /**
     * Retrieves the current warehouse configuration within transfer settings.
     * @return A ResponseEntity containing the Warehouse domain object.
     */
    @GetMapping("/warehouse")
    @Operation(summary = "Get current warehouse configuration",
            description = "Retrieves the complete current warehouse configuration properties within transfer settings.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully retrieved warehouse configuration"),
            @ApiResponse(responseCode = "404", description = "TransferConfig object not found or Warehouse is null"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<Warehouse> getWarehouseConfig() {
        log.info("Received request to get warehouse configuration.");
        HmsMirrorConfig currentHmsMirrorConfig = configService.getConfiguration();
        if (currentHmsMirrorConfig.getTransfer() != null && currentHmsMirrorConfig.getTransfer().getWarehouse() != null) {
            Warehouse warehouse = currentHmsMirrorConfig.getTransfer().getWarehouse();
            log.info("Returning warehouse configuration: {}", warehouse);
            return ResponseEntity.ok(warehouse);
        } else {
            log.warn("Warehouse configuration not found under transfer.");
            return ResponseEntity.notFound().build();
        }
    }

    /**
     * Updates the warehouse configuration within transfer settings.
     * Only provided (non-null) fields in the request body will update the existing configuration.
     * @param warehouse A partial Warehouse domain object with fields to update.
     * @return A ResponseEntity containing the updated Warehouse domain object.
     */
    @PutMapping("/warehouse")
    @Operation(summary = "Update warehouse configuration",
            description = "Updates specific properties within the warehouse configuration. " +
                    "Only provided (non-null) fields will be updated.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully updated warehouse configuration"),
            @ApiResponse(responseCode = "400", description = "Invalid request body"),
            @ApiResponse(responseCode = "404", description = "TransferConfig object not found"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<Warehouse> updateWarehouseConfig(@RequestBody Warehouse warehouse) {
        log.info("Received request to update warehouse configuration with data: {}", warehouse);
        HmsMirrorConfig currentHmsMirrorConfig = configService.getConfiguration();
        if (currentHmsMirrorConfig.getTransfer() == null) {
            log.warn("TransferConfig object not found for warehouse update. Cannot proceed.");
            return ResponseEntity.notFound().build();
        }
        if (currentHmsMirrorConfig.getTransfer().getWarehouse() == null) {
            currentHmsMirrorConfig.getTransfer().setWarehouse(new Warehouse());
            log.debug("Warehouse object was null, initialized a new one.");
        }
        configService.updateWarehouse(currentHmsMirrorConfig.getTransfer().getWarehouse(), warehouse);
        log.info("Warehouse configuration updated successfully.");
        return ResponseEntity.ok(currentHmsMirrorConfig.getTransfer().getWarehouse());
    }
}
