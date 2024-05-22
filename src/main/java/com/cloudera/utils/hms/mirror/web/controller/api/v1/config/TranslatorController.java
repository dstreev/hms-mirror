/*
 * Copyright (c) 2024. Cloudera, Inc. All Rights Reserved
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

package com.cloudera.utils.hms.mirror.web.controller.api.v1.config;

import com.cloudera.utils.hms.mirror.domain.Translator;
import com.cloudera.utils.hms.mirror.service.ExecuteSessionService;
import com.cloudera.utils.hms.mirror.web.service.WebConfigService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@Slf4j
@RequestMapping(path = "/api/v1/translator")
public class TranslatorController {

    private WebConfigService webConfigService;
    private ExecuteSessionService executeSessionService;

    @Autowired
    public void setConfigService(WebConfigService webConfigService) {
        this.webConfigService = webConfigService;
    }

    @Autowired
    public void setHmsMirrorCfgService(ExecuteSessionService executeSessionService) {
        this.executeSessionService = executeSessionService;
    }

    @Operation(summary = "Get Translator Details")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Translator Details set",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = Translator.class))})})
    @ResponseBody
    @RequestMapping(method = RequestMethod.GET, value = "/")
    public Translator getTransfer() {
        log.debug("Getting Translator Details");
        return executeSessionService.getActiveSession().getResolvedConfig().getTranslator();
    }

    // Translator
    @Operation(summary = "Set Translator Details")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Translator Details set",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = Translator.class))})})
    @ResponseBody
    @RequestMapping(method = RequestMethod.PUT, value = "/")
    public Translator setTransfer(
            @RequestParam(value = "forceExternalLocation", required = false) Boolean forceExternalLocation) {
        if (forceExternalLocation != null) {
            log.info("Setting Translator 'forceExternalLocation' to: {}", forceExternalLocation);
            executeSessionService.getActiveSession().getResolvedConfig().getTranslator().setForceExternalLocation(forceExternalLocation);
        }
        return executeSessionService.getActiveSession().getResolvedConfig().getTranslator();
    }

    // Translator / Global Location Map
    // Do this through multiple calls with POST, DELETE to add and remove entries.
    @Operation(summary = "Set Global Location Map Value")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Translator Details set",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = Map.class))})})
    @ResponseBody
    @RequestMapping(method = RequestMethod.POST, value = "/globalLocationMap")
    public Map<String, String> addGLMEntries(
            @RequestParam() Map<String, String> map) {
        log.info("Adding Global Location Map Entries: {}", map);
        map.forEach((k, v) -> executeSessionService.getActiveSession().getResolvedConfig().getTranslator().addGlobalLocationMap(k, v));
        return executeSessionService.getActiveSession().getResolvedConfig().getTranslator().getOrderedGlobalLocationMap();
    }

    @Operation(summary = "Remove Global Location Map Value(s)")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Translator Details set",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = List.class))})})
    @ResponseBody
    @RequestMapping(method = RequestMethod.DELETE, value = "/globalLocationMap")
    public List<String> removeGLMEntries(
            @RequestParam(value = "keys", required = false) List<String> keyList,
            @RequestParam(value = "key", required = false) String key) {
        List<String> removeList = Collections.emptyList();
        if (keyList != null) {
            log.info("Removing Global Location Map Entries: {}", keyList);
            removeList = executeSessionService.getActiveSession().getResolvedConfig().getTranslator().removeGlobalLocationMap(keyList);
        }
        if (key != null) {
            log.info("Removing Global Location Map Entry: {}", key);
            String removedItem = executeSessionService.getActiveSession().getResolvedConfig().getTranslator().removeGlobalLocationMap(key);
            removeList = new ArrayList<>();
            removeList.add(removedItem);
        }
        return removeList;
    }

    @Operation(summary = "Build Global Location Map from Databases.  Resets list!")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "GLM Details set",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = Map.class))})})
    @ResponseBody
    @RequestMapping(method = RequestMethod.POST, value = "/globalLocationMap/build")
    public Map<String, String> buildGLMFromDatabase () {
        // Clear current List
        executeSessionService.getActiveSession().getResolvedConfig().getTranslator().getGlobalLocationMap().clear();
        // Pull all unique locations from databases/tables.  Requires Metastore Direct connection.

        return executeSessionService.getActiveSession().getResolvedConfig().getTranslator().getGlobalLocationMap();
    }
}
