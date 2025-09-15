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

import java.util.Map;

/**
 * REST Controller for managing the 'icebergConversion' section of the HmsMirror configuration.
 * Provides endpoints to retrieve and update Iceberg conversion-related settings.
 */
@RestController("IcebergConversionControllerV2")
@RequestMapping("/api/v2/config/icebergConversion")
@Tag(name = "Iceberg Conversion Configuration", description = "Operations related to Iceberg table conversion properties.")
@Slf4j
public class IcebergConversionController {

    @Autowired
    private ConfigurationService configService;

    /**
     * Retrieves the current Iceberg conversion configuration.
     * @return A ResponseEntity containing the IcebergConversion domain object.
     */
    @GetMapping
    @Operation(summary = "Get current Iceberg conversion configuration",
            description = "Retrieves the complete current Iceberg conversion configuration properties.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully retrieved Iceberg conversion configuration"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<IcebergConversion> getIcebergConversionConfig() {
        log.info("Received request to get Iceberg conversion configuration.");
        IcebergConversion icebergConversion = configService.getConfiguration().getIcebergConversion();
        log.info("Returning Iceberg conversion configuration: {}", icebergConversion);
        return ResponseEntity.ok(icebergConversion);
    }

    /**
     * Updates the Iceberg conversion configuration.
     * Only provided (non-null) fields in the request body will update the existing configuration.
     * @param icebergConversion A partial IcebergConversion domain object with fields to update.
     * @return A ResponseEntity containing the updated IcebergConversion domain object.
     */
    @PutMapping
    @Operation(summary = "Update Iceberg conversion configuration",
            description = "Updates specific properties within the Iceberg conversion configuration. " +
                    "Only provided (non-null) fields will be updated. Map operations require separate endpoints.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully updated Iceberg conversion configuration"),
            @ApiResponse(responseCode = "400", description = "Invalid request body"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<IcebergConversion> updateIcebergConversionConfig(@RequestBody IcebergConversion icebergConversion) {
        log.info("Received request to update Iceberg conversion configuration with data: {}", icebergConversion);
        HmsMirrorConfig currentHmsMirrorConfig = configService.getConfiguration();
        if (currentHmsMirrorConfig.getIcebergConversion() == null) {
            currentHmsMirrorConfig.setIcebergConversion(new IcebergConversion());
            log.debug("IcebergConversion object was null, initialized a new one.");
        }
        configService.updateIcebergConversion(currentHmsMirrorConfig.getIcebergConversion(), icebergConversion);
        log.info("Iceberg conversion configuration updated successfully.");
        return ResponseEntity.ok(currentHmsMirrorConfig.getIcebergConversion());
    }

    /*
     * Endpoints for 'tableProperties'
     */

    /**
     * Retrieves the map of Iceberg table properties.
     * @return A ResponseEntity containing a map of string key-value pairs.
     */
    @GetMapping("/tableProperties")
    @Operation(summary = "Get Iceberg table properties",
            description = "Retrieves the map of properties to be applied during Iceberg table conversion.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully retrieved map"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<Map<String, String>> getTableProperties() {
        log.info("Received request to get Iceberg table properties.");
        Map<String, String> properties = configService.getIcebergTableProperties();
        log.info("Returning Iceberg table properties with {} entries.", properties.size());
        return ResponseEntity.ok(properties);
    }

    /**
     * Clears all items from the Iceberg table properties map.
     * @return A ResponseEntity indicating success.
     */
    @DeleteMapping("/tableProperties/clear")
    @Operation(summary = "Clear Iceberg table properties",
            description = "Removes all entries from the Iceberg table properties map.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Successfully cleared map"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<Void> clearTableProperties() {
        log.info("Received request to clear Iceberg table properties.");
        configService.clearIcebergTableProperties();
        log.info("Iceberg table properties cleared successfully.");
        return ResponseEntity.noContent().build();
    }

    /**
     * Adds or updates an Iceberg table property.
     * @param key The key of the property.
     * @param value The value of the property.
     * @return A ResponseEntity indicating success.
     */
    @PostMapping("/tableProperties/{key}")
    @Operation(summary = "Add/Update Iceberg table property",
            description = "Adds a new or updates an existing Iceberg table property.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully added/updated property"),
            @ApiResponse(responseCode = "400", description = "Invalid request body or key"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<String> addOrUpdateTableProperty(@PathVariable String key, @RequestBody String value) {
        log.info("Received request to add/update Iceberg table property: key='{}', value='{}'", key, value);
        configService.addIcebergTableProperty(key, value);
        log.info("Iceberg table property key='{}' set to value='{}'.", key, value);
        return ResponseEntity.ok("Property '" + key + "' set to '" + value + "'.");
    }

    /**
     * Deletes an Iceberg table property by its key.
     * @param key The key of the property to delete.
     * @return A ResponseEntity indicating success or if the property was not found.
     */
    @DeleteMapping("/tableProperties/{key}")
    @Operation(summary = "Delete Iceberg table property",
            description = "Deletes an Iceberg table property by its key.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully deleted property or property not found"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<String> deleteTableProperty(@PathVariable String key) {
        log.info("Received request to delete Iceberg table property for key: '{}'", key);
        Map<String, String> properties = configService.getIcebergTableProperties();
        if (properties.containsKey(key)) {
            configService.deleteIcebergTableProperty(key);
            log.info("Iceberg table property key='{}' deleted successfully.", key);
            return ResponseEntity.ok("Property '" + key + "' deleted.");
        } else {
            log.warn("Iceberg table property key='{}' not found. No deletion performed.", key);
            return ResponseEntity.ok("Property '" + key + "' not found.");
        }
    }
}
