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
import com.cloudera.utils.hms.mirror.domain.support.TableType; // Import TableType enum
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
 * REST Controller for managing the 'translator' section of the HmsMirror configuration.
 * Includes nested 'warehouseMapBuilder' settings and its maps.
 */
@RestController("TranslatorControllerV2")
@RequestMapping("/api/v2/config/translator")
@Tag(name = "Translator Configuration", description = "Operations related to translator properties, including global location maps and warehouse map builder.")
@Slf4j
public class TranslatorController {

    @Autowired
    private ConfigurationService configService;

    /**
     * Retrieves the current translator configuration.
     * @return A ResponseEntity containing the Translator domain object.
     */
    @GetMapping
    @Operation(summary = "Get current translator configuration",
            description = "Retrieves the complete current translator configuration properties.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully retrieved translator configuration"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<Translator> getTranslatorConfig() {
        log.info("Received request to get translator configuration.");
        Translator translator = configService.getConfiguration().getTranslator();
        log.info("Returning translator configuration: {}", translator);
        return ResponseEntity.ok(translator);
    }

    /**
     * Updates the translator configuration.
     * Only provided (non-null) fields in the request body will update the existing configuration.
     * @param translator A partial Translator domain object with fields to update.
     * @return A ResponseEntity containing the updated Translator domain object.
     */
    @PutMapping
    @Operation(summary = "Update translator configuration",
            description = "Updates specific properties within the translator configuration. " +
                    "Only provided (non-null) fields will be updated.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully updated translator configuration"),
            @ApiResponse(responseCode = "400", description = "Invalid request body"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<Translator> updateTranslatorConfig(@RequestBody Translator translator) {
        log.info("Received request to update translator configuration with data: {}", translator);
        HmsMirrorConfig currentHmsMirrorConfig = configService.getConfiguration();
        if (currentHmsMirrorConfig.getTranslator() == null) {
            currentHmsMirrorConfig.setTranslator(new Translator());
            log.debug("Translator object was null, initialized a new one.");
        }
        configService.updateTranslator(currentHmsMirrorConfig.getTranslator(), translator);
        log.info("Translator configuration updated successfully.");
        return ResponseEntity.ok(currentHmsMirrorConfig.getTranslator());
    }

    /**
     * Retrieves the current warehouse map builder configuration within translator settings.
     * @return A ResponseEntity containing the WarehouseMapBuilder domain object.
     */
    @GetMapping("/warehouseMapBuilder")
    @Operation(summary = "Get current warehouse map builder configuration",
            description = "Retrieves the complete current warehouse map builder configuration properties within translator settings.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully retrieved warehouse map builder configuration"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<WarehouseMapBuilder> getWarehouseMapBuilderConfig() {
        log.info("Received request to get warehouse map builder configuration.");
        WarehouseMapBuilder warehouseMapBuilder = configService.getConfiguration().getTranslator().getWarehouseMapBuilder();
        log.info("Returning warehouse map builder configuration: {}", warehouseMapBuilder);
        return ResponseEntity.ok(warehouseMapBuilder);
    }

    /**
     * Updates the warehouse map builder configuration within translator settings.
     * Only provided (non-null) fields in the request body will update the existing configuration.
     * @param warehouseMapBuilder A partial WarehouseMapBuilder domain object with fields to update.
     * @return A ResponseEntity containing the updated WarehouseMapBuilder domain object.
     */
    @PutMapping("/warehouseMapBuilder")
    @Operation(summary = "Update warehouse map builder configuration",
            description = "Updates specific properties within the warehouse map builder configuration. " +
                    "Only provided (non-null) fields will be updated. Map operations require separate endpoints.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully updated warehouse map builder configuration"),
            @ApiResponse(responseCode = "400", description = "Invalid request body"),
            @ApiResponse(responseCode = "404", description = "Translator object not found"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<WarehouseMapBuilder> updateWarehouseMapBuilderConfig(@RequestBody WarehouseMapBuilder warehouseMapBuilder) {
        log.info("Received request to update warehouse map builder configuration with data: {}", warehouseMapBuilder);
        HmsMirrorConfig currentHmsMirrorConfig = configService.getConfiguration();
        if (currentHmsMirrorConfig.getTranslator() == null) {
            log.warn("Translator object not found for warehouse map builder update. Cannot proceed.");
            return ResponseEntity.notFound().build();
        }
        if (currentHmsMirrorConfig.getTranslator().getWarehouseMapBuilder() == null) {
            currentHmsMirrorConfig.getTranslator().setWarehouseMapBuilder(new WarehouseMapBuilder());
            log.debug("WarehouseMapBuilder object was null, initialized a new one.");
        }
        configService.updateWarehouseMapBuilder(currentHmsMirrorConfig.getTranslator().getWarehouseMapBuilder(), warehouseMapBuilder);
        log.info("Warehouse map builder configuration updated successfully.");
        return ResponseEntity.ok(currentHmsMirrorConfig.getTranslator().getWarehouseMapBuilder());
    }

    /*
     * Endpoints for 'translator.warehouseMapBuilder.sources' map
     */

    @GetMapping("/warehouseMapBuilder/sources")
    @Operation(summary = "Get warehouse map builder sources",
            description = "Retrieves the map of source locations within the warehouse map builder. Key is database name, value is SourceLocationMap.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully retrieved map"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<Map<String, SourceLocationMap>> getWarehouseMapBuilderSources() {
        log.info("Received request to get warehouse map builder sources.");
        Map<String, SourceLocationMap> sources = configService.getTranslatorWarehouseSources();
        log.info("Returning warehouse map builder sources with {} entries.", sources.size());
        return ResponseEntity.ok(sources);
    }

    @PostMapping("/warehouseMapBuilder/sources/{dbName}")
    @Operation(summary = "Add or update a warehouse map builder source for a database",
            description = "Adds a new or updates an existing SourceLocationMap for a specific database in the warehouse map builder sources.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully added/updated source"),
            @ApiResponse(responseCode = "400", description = "Invalid request body or database name"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<String> addOrUpdateWarehouseMapBuilderSource(@PathVariable String dbName, @RequestBody SourceLocationMap sourceLocationMap) {
        log.info("Received request to add/update warehouse map builder source for database: {}", dbName);
        configService.addTranslatorWarehouseSource(dbName, sourceLocationMap);
        log.info("Warehouse map builder source for database '{}' updated successfully.", dbName);
        return ResponseEntity.ok("Source for database '" + dbName + "' added/updated.");
    }

    @DeleteMapping("/warehouseMapBuilder/sources/{dbName}")
    @Operation(summary = "Delete a warehouse map builder source for a database",
            description = "Deletes the SourceLocationMap for a specific database from the warehouse map builder sources.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully deleted source or source not found"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<String> deleteWarehouseMapBuilderSource(@PathVariable String dbName) {
        log.info("Received request to delete warehouse map builder source for database: '{}'", dbName);
        Map<String, SourceLocationMap> sources = configService.getTranslatorWarehouseSources();
        if (sources.containsKey(dbName)) {
            configService.deleteTranslatorWarehouseSource(dbName);
            log.info("Warehouse map builder source for database '{}' deleted successfully.", dbName);
            return ResponseEntity.ok("Source for database '" + dbName + "' deleted.");
        } else {
            log.warn("Warehouse map builder source for database '{}' not found. No deletion performed.", dbName);
            return ResponseEntity.ok("Source for database '" + dbName + "' not found.");
        }
    }

    @DeleteMapping("/warehouseMapBuilder/sources/clear")
    @Operation(summary = "Clear all warehouse map builder sources",
            description = "Removes all entries from the warehouse map builder sources map.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Successfully cleared map"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<Void> clearWarehouseMapBuilderSources() {
        log.info("Received request to clear all warehouse map builder sources.");
        configService.clearTranslatorWarehouseSources();
        log.info("Warehouse map builder sources cleared successfully.");
        return ResponseEntity.noContent().build();
    }


    /*
     * Endpoints for 'translator.warehouseMapBuilder.warehousePlans' map
     */

    @GetMapping("/warehouseMapBuilder/warehousePlans")
    @Operation(summary = "Get warehouse map builder plans",
            description = "Retrieves the map of warehouse plans for the warehouse map builder. Key is database name, value is Warehouse object.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully retrieved map"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<Map<String, Warehouse>> getWarehouseMapBuilderPlans() {
        log.info("Received request to get warehouse map builder plans.");
        Map<String, Warehouse> plans = configService.getTranslatorWarehousePlans();
        log.info("Returning warehouse map builder plans with {} entries.", plans.size());
        return ResponseEntity.ok(plans);
    }

    @PostMapping("/warehouseMapBuilder/warehousePlans/{dbName}")
    @Operation(summary = "Add or update a warehouse map builder plan for a database",
            description = "Adds a new or updates an existing Warehouse object for a specific database in the warehouse map builder plans.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully added/updated plan"),
            @ApiResponse(responseCode = "400", description = "Invalid request body or database name"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<String> addOrUpdateWarehouseMapBuilderPlan(@PathVariable String dbName, @RequestBody Warehouse warehouse) {
        log.info("Received request to add/update warehouse map builder plan for database: {}", dbName);
        configService.addTranslatorWarehousePlan(dbName, warehouse);
        log.info("Warehouse map builder plan for database '{}' updated successfully.", dbName);
        return ResponseEntity.ok("Plan for database '" + dbName + "' added/updated.");
    }

    @DeleteMapping("/warehouseMapBuilder/warehousePlans/{dbName}")
    @Operation(summary = "Delete a warehouse map builder plan for a database",
            description = "Deletes the Warehouse object for a specific database from the warehouse map builder plans.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully deleted plan or plan not found"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<String> deleteWarehouseMapBuilderPlan(@PathVariable String dbName) {
        log.info("Received request to delete warehouse map builder plan for database: '{}'", dbName);
        Map<String, Warehouse> plans = configService.getTranslatorWarehousePlans();
        if (plans.containsKey(dbName)) {
            configService.deleteTranslatorWarehousePlan(dbName);
            log.info("Warehouse map builder plan for database '{}' deleted successfully.", dbName);
            return ResponseEntity.ok("Plan for database '" + dbName + "' deleted.");
        } else {
            log.warn("Warehouse map builder plan for database '{}' not found. No deletion performed.", dbName);
            return ResponseEntity.ok("Plan for database '" + dbName + "' not found.");
        }
    }

    @DeleteMapping("/warehouseMapBuilder/warehousePlans/clear")
    @Operation(summary = "Clear all warehouse map builder plans",
            description = "Removes all entries from the warehouse map builder plans map.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Successfully cleared map"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<Void> clearWarehouseMapBuilderPlans() {
        log.info("Received request to clear all warehouse map builder plans.");
        configService.clearTranslatorWarehousePlans();
        log.info("Warehouse map builder plans cleared successfully.");
        return ResponseEntity.noContent().build();
    }
}
