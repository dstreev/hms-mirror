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

import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.feature.*; // Import domain object
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
 * REST Controller for managing the 'legacyTranslations' section of the HmsMirror configuration.
 * Specifically handles 'rowSerde' map operations.
 */
@RestController("LegacyTranslationsControllerV2")
@RequestMapping("/api/v2/config/legacyTranslations")
@Tag(name = "Legacy Translations Configuration", description = "Operations related to legacy translation properties, specifically for rowSerde.")
@Slf4j
public class LegacyTranslationsController {

    @Autowired
    private ConfigurationService configService;

    /**
     * Retrieves the current legacy translations configuration.
     * @return A ResponseEntity containing the LegacyTranslations domain object.
     */
    @GetMapping
    @Operation(summary = "Get current legacy translations configuration",
            description = "Retrieves the complete current legacy translations configuration properties.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully retrieved legacy translations configuration"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<LegacyTranslations> getLegacyTranslationsConfig() {
        log.info("Received request to get legacy translations configuration.");
        LegacyTranslations legacyTranslations = configService.getConfiguration().getLegacyTranslations();
        log.info("Returning legacy translations configuration: {}", legacyTranslations);
        return ResponseEntity.ok(legacyTranslations);
    }

    /**
     * Updates the legacy translations configuration.
     * Only provided (non-null) fields in the request body will update the existing configuration.
     * @param legacyTranslations A partial LegacyTranslations domain object with fields to update.
     * @return A ResponseEntity containing the updated LegacyTranslations domain object.
     */
    @PutMapping
    @Operation(summary = "Update legacy translations configuration",
            description = "Updates specific properties within the legacy translations configuration. " +
                    "Only provided (non-null) fields will be updated. Map operations require separate endpoints.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully updated legacy translations configuration"),
            @ApiResponse(responseCode = "400", description = "Invalid request body"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<LegacyTranslations> updateLegacyTranslationsConfig(@RequestBody LegacyTranslations legacyTranslations) {
        log.info("Received request to update legacy translations configuration with data: {}", legacyTranslations);
        HmsMirrorConfig currentHmsMirrorConfig = configService.getConfiguration();
        if (currentHmsMirrorConfig.getLegacyTranslations() == null) {
            currentHmsMirrorConfig.setLegacyTranslations(new LegacyTranslations());
            log.debug("LegacyTranslations object was null, initialized a new one.");
        }
        configService.updateLegacyTranslations(currentHmsMirrorConfig.getLegacyTranslations(), legacyTranslations);
        log.info("Legacy translations configuration updated successfully.");
        return ResponseEntity.ok(currentHmsMirrorConfig.getLegacyTranslations());
    }

    /*
     * Endpoints for 'rowSerde' map
     */

    /**
     * Retrieves the rowSerde translation map.
     * @return A ResponseEntity containing a map of string key-value pairs.
     */
    @GetMapping("/rowSerde")
    @Operation(summary = "Get rowSerde translations",
            description = "Retrieves the map of legacy row SerDe class translations.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully retrieved map"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<Map<String, String>> getRowSerdeTranslations() {
        log.info("Received request to get rowSerde translations.");
        Map<String, String> translations = configService.getRowSerdeTranslations();
        log.info("Returning rowSerde translations with {} entries.", translations.size());
        return ResponseEntity.ok(translations);
    }

    /**
     * Clears all items from the rowSerde translation map.
     * @return A ResponseEntity indicating success.
     */
    @DeleteMapping("/rowSerde/clear")
    @Operation(summary = "Clear rowSerde translations",
            description = "Removes all entries from the rowSerde translation map.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Successfully cleared map"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<Void> clearRowSerdeTranslations() {
        log.info("Received request to clear rowSerde translations.");
        configService.clearRowSerdeTranslations();
        log.info("RowSerde translations cleared successfully.");
        return ResponseEntity.noContent().build();
    }

    /**
     * Adds or updates a rowSerde translation.
     * @param key The key (original SerDe class name) of the translation.
     * @param value The value (new SerDe class name) of the translation.
     * @return A ResponseEntity indicating success.
     */
    @PostMapping("/rowSerde/{key}")
    @Operation(summary = "Add/Update rowSerde translation",
            description = "Adds a new or updates an existing row SerDe translation.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully added/updated translation"),
            @ApiResponse(responseCode = "400", description = "Invalid request body or key"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<String> addOrUpdateRowSerdeTranslation(@PathVariable String key, @RequestBody String value) {
        log.info("Received request to add/update rowSerde translation: key='{}', value='{}'", key, value);
        configService.addRowSerdeTranslation(key, value);
        log.info("RowSerde translation key='{}' set to value='{}'.", key, value);
        return ResponseEntity.ok("Translation for key '" + key + "' set to '" + value + "'.");
    }

    /**
     * Deletes a rowSerde translation by its key.
     * @param key The key (original SerDe class name) of the translation to delete.
     * @return A ResponseEntity indicating success or if the translation was not found.
     */
    @DeleteMapping("/rowSerde/{key}")
    @Operation(summary = "Delete rowSerde translation",
            description = "Deletes a row SerDe translation by its key.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully deleted translation or translation not found"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<String> deleteRowSerdeTranslation(@PathVariable String key) {
        log.info("Received request to delete rowSerde translation for key: '{}'", key);
        Map<String, String> translations = configService.getRowSerdeTranslations();
        if (translations.containsKey(key)) {
            configService.deleteRowSerdeTranslation(key);
            log.info("RowSerde translation for key='{}' deleted successfully.", key);
            return ResponseEntity.ok("Translation for key '" + key + "' deleted.");
        } else {
            log.warn("Translation for key='{}' not found. No deletion performed.", key);
            return ResponseEntity.ok("Translation for key '" + key + "' not found.");
        }
    }
}
