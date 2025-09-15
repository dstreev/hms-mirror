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

import java.util.List;

/**
 * REST Controller for managing the 'filter' section of the HmsMirror configuration.
 * Provides endpoints to retrieve and update filter-related settings, including list management for dbPropertySkipList.
 */
@RestController("FilterControllerV2")
@RequestMapping("/api/v2/config/filter")
@Tag(name = "Filter Configuration", description = "Operations related to the filter configuration properties, including database and table regular expressions and skip lists.")
@Slf4j
public class FilterController {

    @Autowired
    private ConfigurationService configService;

    /**
     * Retrieves the current filter configuration.
     * @return A ResponseEntity containing the Filter domain object.
     */
    @GetMapping
    @Operation(summary = "Get current filter configuration",
            description = "Retrieves the complete current filter configuration properties.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully retrieved filter configuration"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<Filter> getFilterConfig() {
        log.info("Received request to get filter configuration.");
        Filter filter = configService.getConfiguration().getFilter();
        log.info("Returning filter configuration: {}", filter);
        return ResponseEntity.ok(filter);
    }

    /**
     * Updates the filter configuration.
     * Only provided (non-null) fields in the request body will update the existing configuration.
     * @param filter A partial Filter domain object with fields to update.
     * @return A ResponseEntity containing the updated Filter domain object.
     */
    @PutMapping
    @Operation(summary = "Update filter configuration",
            description = "Updates specific properties within the filter configuration. " +
                    "Only provided (non-null) fields will be updated. List operations require separate endpoints.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully updated filter configuration"),
            @ApiResponse(responseCode = "400", description = "Invalid request body"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<Filter> updateFilterConfig(@RequestBody Filter filter) {
        log.info("Received request to update filter configuration with data: {}", filter);
        HmsMirrorConfig currentHmsMirrorConfig = configService.getConfiguration();
        if (currentHmsMirrorConfig.getFilter() == null) {
            currentHmsMirrorConfig.setFilter(new Filter());
            log.debug("Filter object was null, initialized a new one.");
        }
        configService.updateFilter(currentHmsMirrorConfig.getFilter(), filter);
        log.info("Filter configuration updated successfully.");
        return ResponseEntity.ok(currentHmsMirrorConfig.getFilter());
    }

    /*
     * Endpoints for 'dbPropertySkipList'
     */

    /**
     * Retrieves the list of database properties to skip.
     * @return A ResponseEntity containing a list of strings.
     */
    @GetMapping("/dbPropertySkipList")
    @Operation(summary = "Get database property skip list",
            description = "Retrieves the list of database properties that should be skipped during processing.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully retrieved list"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<List<String>> getDbPropertySkipList() {
        log.info("Received request to get database property skip list.");
        List<String> list = configService.getDbPropertySkipList();
        log.info("Returning database property skip list with {} items.", list.size());
        return ResponseEntity.ok(list);
    }

    /**
     * Clears all items from the database property skip list.
     * @return A ResponseEntity indicating success.
     */
    @DeleteMapping("/dbPropertySkipList/clear")
    @Operation(summary = "Clear database property skip list",
            description = "Removes all entries from the database property skip list.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Successfully cleared list"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<Void> clearDbPropertySkipList() {
        log.info("Received request to clear database property skip list.");
        configService.clearDbPropertySkipList();
        log.info("Database property skip list cleared successfully.");
        return ResponseEntity.noContent().build();
    }

    /**
     * Adds an item to the database property skip list.
     * @param item The string item to add.
     * @return A ResponseEntity indicating success or if the item already exists.
     */
    @PostMapping("/dbPropertySkipList")
    @Operation(summary = "Add item to database property skip list",
            description = "Adds a new entry to the database property skip list if it doesn't already exist.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully added item or item already exists"),
            @ApiResponse(responseCode = "400", description = "Invalid request body"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<String> addDbPropertySkipListItem(@RequestBody String item) {
        log.info("Received request to add item '{}' to database property skip list.", item);
        if (configService.addDbPropertySkipListItem(item)) {
            log.info("Item '{}' added successfully to database property skip list.", item);
            return ResponseEntity.ok("Item '" + item + "' added.");
        } else {
            log.warn("Item '{}' already exists in database property skip list. No action taken.", item);
            return ResponseEntity.ok("Item '" + item + "' already exists.");
        }
    }

    /**
     * Deletes an item from the database property skip list.
     * @param item The string item to delete.
     * @return A ResponseEntity indicating success or if the item was not found.
     */
    @DeleteMapping("/dbPropertySkipList")
    @Operation(summary = "Delete item from database property skip list",
            description = "Removes an entry from the database property skip list if it exists.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully deleted item or item not found"),
            @ApiResponse(responseCode = "400", description = "Invalid request body"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<String> deleteDbPropertySkipListItem(@RequestBody String item) {
        log.info("Received request to delete item '{}' from database property skip list.", item);
        if (configService.deleteDbPropertySkipListItem(item)) {
            log.info("Item '{}' deleted successfully from database property skip list.", item);
            return ResponseEntity.ok("Item '" + item + "' deleted.");
        } else {
            log.warn("Item '{}' not found in database property skip list. No deletion performed.", item);
            return ResponseEntity.ok("Item '" + item + "' not found.");
        }
    }
}
