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
import com.cloudera.utils.hms.mirror.exceptions.MismatchException;
import com.cloudera.utils.hms.mirror.service.ExecuteSessionService;
import com.cloudera.utils.hms.mirror.service.TranslatorService;
import com.cloudera.utils.hms.mirror.web.service.WebConfigService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.validation.constraints.NotNull;
import java.util.*;

@CrossOrigin
@RestController
@Slf4j
@RequestMapping(path = "/api/v1/translator")
public class TranslatorController {

    private ExecuteSessionService executeSessionService;
    private TranslatorService translatorService;

    @Autowired
    public void setHmsMirrorCfgService(ExecuteSessionService executeSessionService) {
        this.executeSessionService = executeSessionService;
    }

    @Autowired
    public void setTranslatorService(TranslatorService translatorService) {
        this.translatorService = translatorService;
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
            @RequestParam(value = "forceExternalLocation", required = false) Boolean forceExternalLocation ) {
        if (forceExternalLocation != null) {
            log.info("Setting Translator 'forceExternalLocation' to: {}", forceExternalLocation);
            executeSessionService.getActiveSession().getResolvedConfig().getTranslator().setForceExternalLocation(forceExternalLocation);
        }
        return executeSessionService.getActiveSession().getResolvedConfig().getTranslator();
    }

    // Translator / Global Location Map
    // Do this through multiple calls with POST, DELETE to add and remove entries.


//    @Operation(summary = "Build Global Location Map from Databases.  Resets list!")
//    @ApiResponses(value = {
//            @ApiResponse(responseCode = "200", description = "GLM Details set",
//                    content = {@Content(mediaType = "application/json",
//                            schema = @Schema(implementation = Map.class))})})
//    @ResponseBody
//    @RequestMapping(method = RequestMethod.POST, value = "/globalLocationMap/build")
//    public Map<String, String> buildGLMFromDatabase () {
//        // Clear current List
//        executeSessionService.getActiveSession().getResolvedConfig().getTranslator().getGlobalLocationMap().clear();
//        // Pull all unique locations from databases/tables.  Requires Metastore Direct connection.
//
//        return executeSessionService.getActiveSession().getResolvedConfig().getTranslator().getGlobalLocationMap();
//    }
//

    @Operation(summary = "Add GLM")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "GLM Added",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema())}),
            @ApiResponse(responseCode = "400", description = "Invalid environment supplied",
                    content = @Content),
            @ApiResponse(responseCode = "404", description = "Cluster not found",
                    content = @Content)})
    @ResponseBody
    @RequestMapping(method = RequestMethod.POST, value = "/globalLocationMap")
    public void addGlobalLocationMap(@RequestParam(name = "source", required = true) String source,
                                     @RequestParam(name = "target", required = true) String target) {
        log.info("Adding global location map for source: {} and target: {}", source, target);
        translatorService.addGlobalLocationMap(source, target);
    }

    @Operation(summary = "Remove GLM")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "GLM Added",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = String.class))}),
            @ApiResponse(responseCode = "400", description = "Invalid environment supplied",
                    content = @Content),
            @ApiResponse(responseCode = "404", description = "Cluster not found",
                    content = @Content)})
    @ResponseBody
    @RequestMapping(method = RequestMethod.DELETE, value = "/globalLocationMap")
    public String removeGlobalLocationMap(@RequestParam(name = "source", required = true) String source) {
        log.info("Removing global location map for source: {}", source);
        return translatorService.removeGlobalLocationMap(source);
    }

    // Get Global Location Map
    @Operation(summary = "Get GLM's")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "GLM's retrieved",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = Map.class))}),
            @ApiResponse(responseCode = "400", description = "Invalid environment supplied",
                    content = @Content),
            @ApiResponse(responseCode = "404", description = "Cluster not found",
                    content = @Content)})
    @ResponseBody
    @RequestMapping(method = RequestMethod.GET, value = "/globalLocationMap/list")
    public Map<String, String> getGlobalLocationMaps() {
        log.info("Getting global location maps");
        return translatorService.getGlobalLocationMap();
    }

    // Get Global Location Map
    @Operation(summary = "Build GLM from Warehouse Plan")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "GLM Built",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = Map.class))}),
            @ApiResponse(responseCode = "400", description = "Invalid environment supplied",
                    content = @Content),
            @ApiResponse(responseCode = "404", description = "Cluster not found",
                    content = @Content)})
    @ResponseBody
    @RequestMapping(method = RequestMethod.POST, value = "/globalLocationMap/build")
    public Map<String, String> buildGLMFromPlans(@RequestParam(name = "dryrun", required = false) Boolean dryrun,
                                                 @RequestParam(name = "consolidationLevel", required = false) Integer consolidationLevel)
            throws MismatchException {
        log.info("Building global location maps");
        boolean lclDryrun = dryrun != null ? dryrun : true;
        int lclConsolidationLevel = consolidationLevel != null ? consolidationLevel : 1;
        return translatorService.buildGlobalLocationMapFromWarehousePlansAndSources(lclDryrun, lclConsolidationLevel);
    }


}
