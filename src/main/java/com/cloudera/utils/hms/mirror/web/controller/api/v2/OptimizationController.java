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
import com.cloudera.utils.hms.mirror.domain.support.SideType; // Import SideType enum
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
 * REST Controller for managing the 'optimization' section of the HmsMirror configuration.
 * Provides endpoints to retrieve and update optimization settings, including overrides.
 */
@RestController("OptimizationControllerV2")
@RequestMapping("/api/v2/config/optimization")
@Tag(name = "Optimization Configuration", description = "Operations related to optimization properties.")
@Slf4j
public class OptimizationController {

    @Autowired
    private ConfigurationService configService;

    /**
     * Retrieves the current optimization configuration.
     * @return A ResponseEntity containing the Optimization domain object.
     */
    @GetMapping
    @Operation(summary = "Get current optimization configuration",
            description = "Retrieves the complete current optimization configuration properties.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully retrieved optimization configuration"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<Optimization> getOptimizationConfig() {
        log.info("Received request to get optimization configuration.");
        Optimization optimization = configService.getConfiguration().getOptimization();
        log.info("Returning optimization configuration: {}", optimization);
        return ResponseEntity.ok(optimization);
    }

    /**
     * Updates the optimization configuration.
     * Only provided (non-null) fields in the request body will update the existing configuration.
     * @param optimization A partial Optimization domain object with fields to update.
     * @return A ResponseEntity containing the updated Optimization domain object.
     */
    @PutMapping
    @Operation(summary = "Update optimization configuration",
            description = "Updates specific properties within the optimization configuration. " +
                    "Only provided (non-null) fields will be updated.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully updated optimization configuration"),
            @ApiResponse(responseCode = "400", description = "Invalid request body"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<Optimization> updateOptimizationConfig(@RequestBody Optimization optimization) {
        log.info("Received request to update optimization configuration with data: {}", optimization);
        HmsMirrorConfig currentHmsMirrorConfig = configService.getConfiguration();
        if (currentHmsMirrorConfig.getOptimization() == null) {
            currentHmsMirrorConfig.setOptimization(new Optimization());
            log.debug("Optimization object was null, initialized a new one.");
        }
        configService.updateOptimization(currentHmsMirrorConfig.getOptimization(), optimization);
        log.info("Optimization configuration updated successfully.");
        return ResponseEntity.ok(currentHmsMirrorConfig.getOptimization());
    }

    /**
     * Retrieves the current optimization overrides configuration.
     * @return A ResponseEntity containing the Overrides domain object.
     */
    @GetMapping("/overrides")
    @Operation(summary = "Get current optimization overrides configuration",
            description = "Retrieves the complete current optimization overrides configuration properties.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully retrieved overrides configuration"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<Overrides> getOptimizationOverridesConfig() {
        log.info("Received request to get optimization overrides configuration.");
        Overrides overrides = configService.getConfiguration().getOptimization().getOverrides();
        log.info("Returning optimization overrides configuration: {}", overrides);
        return ResponseEntity.ok(overrides);
    }

    /**
     * Updates the optimization overrides configuration.
     * Only provided (non-null) fields in the request body will update the existing configuration.
     * @param overrides A partial Overrides domain object with fields to update.
     * @return A ResponseEntity containing the updated Overrides domain object.
     */
    @PutMapping("/overrides")
    @Operation(summary = "Update optimization overrides configuration",
            description = "Updates specific properties within the optimization overrides configuration. " +
                    "Only provided (non-null) fields will be updated. Map operations require separate endpoints.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully updated overrides configuration"),
            @ApiResponse(responseCode = "400", description = "Invalid request body"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<Overrides> updateOptimizationOverridesConfig(@RequestBody Overrides overrides) {
        log.info("Received request to update optimization overrides configuration with data: {}", overrides);
        HmsMirrorConfig currentHmsMirrorConfig = configService.getConfiguration();
        if (currentHmsMirrorConfig.getOptimization() == null) {
            currentHmsMirrorConfig.setOptimization(new Optimization());
            log.debug("Optimization object was null, initialized for overrides update.");
        }
        if (currentHmsMirrorConfig.getOptimization().getOverrides() == null) {
            currentHmsMirrorConfig.getOptimization().setOverrides(new Overrides());
            log.debug("Overrides object was null, initialized a new one.");
        }
        configService.updateOverrides(currentHmsMirrorConfig.getOptimization().getOverrides(), overrides);
        log.info("Optimization overrides configuration updated successfully.");
        return ResponseEntity.ok(currentHmsMirrorConfig.getOptimization().getOverrides());
    }

    /*
     * Endpoints for 'optimization.overrides.properties' map
     * This is a Map<String, Map<SideType, String>>
     */

    @GetMapping("/overrides/properties")
    @Operation(summary = "Get optimization override properties",
            description = "Retrieves the map of properties that override default optimization settings.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully retrieved map"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<Map<String, Map<SideType, String>>> getOptimizationOverrideProperties() {
        log.info("Received request to get optimization override properties.");
        Map<String, Map<SideType, String>> properties = configService.getOptimizationOverrideProperties();
        log.info("Returning optimization override properties with {} entries.", properties.size());
        return ResponseEntity.ok(properties);
    }

    @PostMapping("/overrides/properties/{key}/{sideType}")
    @Operation(summary = "Add or update an optimization override property for a specific side",
            description = "Adds a new or updates an existing optimization override property for the specified key and side (LEFT, RIGHT, or BOTH).")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully added/updated property"),
            @ApiResponse(responseCode = "400", description = "Invalid request body, key, or sideType"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<String> addOrUpdateOptimizationOverrideProperty(@PathVariable String key,
                                                                          @PathVariable SideType sideType,
                                                                          @RequestBody String value) {
        log.info("Received request to add/update optimization override property: key='{}', side='{}', value='{}'", key, sideType, value);
        configService.addOptimizationOverrideProperty(key, sideType, value);
        log.info("Optimization override property key='{}' for side '{}' set to value='{}'.", key, sideType, value);
        return ResponseEntity.ok("Property '" + key + "' for side '" + sideType + "' set to '" + value + "'.");
    }

    @DeleteMapping("/overrides/properties/{key}/{sideType}")
    @Operation(summary = "Delete an optimization override property for a specific side",
            description = "Deletes an optimization override property for the specified key and side.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully deleted property or property not found"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<String> deleteOptimizationOverrideProperty(@PathVariable String key,
                                                                      @PathVariable SideType sideType) {
        log.info("Received request to delete optimization override property for key: '{}', side: '{}'", key, sideType);
        Map<String, Map<SideType, String>> properties = configService.getOptimizationOverrideProperties();
        if (properties.containsKey(key) && properties.get(key).containsKey(sideType)) {
            configService.deleteOptimizationOverrideProperty(key, sideType);
            log.info("Optimization override property for key '{}', side '{}' deleted.", key, sideType);
            return ResponseEntity.ok("Property '" + key + "' for side '" + sideType + "' deleted.");
        } else {
            log.warn("Optimization override property for key '{}', side '{}' not found. No deletion performed.", key, sideType);
            return ResponseEntity.ok("Property '" + key + "' for side '" + sideType + "' not found.");
        }
    }

    @DeleteMapping("/overrides/properties/clear")
    @Operation(summary = "Clear all optimization override properties",
            description = "Removes all entries from the optimization override properties map.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Successfully cleared properties"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<Void> clearOptimizationOverrideProperties() {
        log.info("Received request to clear all optimization override properties.");
        configService.clearOptimizationOverrideProperties();
        log.info("All optimization override properties cleared successfully.");
        return ResponseEntity.noContent().build();
    }
}
