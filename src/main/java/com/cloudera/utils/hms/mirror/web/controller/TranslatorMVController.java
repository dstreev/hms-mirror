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

package com.cloudera.utils.hms.mirror.web.controller;

import com.cloudera.utils.hms.mirror.domain.WarehouseMapBuilder;
import com.cloudera.utils.hms.mirror.exceptions.MismatchException;
import com.cloudera.utils.hms.mirror.exceptions.RequiredConfigurationException;
import com.cloudera.utils.hms.mirror.exceptions.SessionRunningException;
import com.cloudera.utils.hms.mirror.service.DatabaseService;
import com.cloudera.utils.hms.mirror.service.ExecuteSessionService;
import com.cloudera.utils.hms.mirror.service.TranslatorService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import java.util.Map;
import javax.validation.constraints.NotNull;

@Controller
@RequestMapping(path = "/translator")
@Slf4j
public class TranslatorMVController {

    private DatabaseService databaseService;
    private ExecuteSessionService executeSessionService;
    private TranslatorService translatorService;

    @Autowired
    public void setDatabaseService(DatabaseService databaseService) {
        this.databaseService = databaseService;
    }

    @Autowired
    public void setExecuteSessionService(ExecuteSessionService executeSessionService) {
        this.executeSessionService = executeSessionService;
    }

    @Autowired
    public void setTranslatorService(TranslatorService translatorService) {
        this.translatorService = translatorService;
    }

    @RequestMapping(value = "/globalLocationMap/{source}/delete", method = RequestMethod.GET)
    public String removeGlobalLocationMap(Model model,
                                          @PathVariable @NotNull String source) throws SessionRunningException {
        // Don't reload if running.
        executeSessionService.clearActiveSession();

        log.info("Removing global location map for source: {}", source);
        translatorService.removeGlobalLocationMap(source);

        return "redirect:/config/view";
    }

    @RequestMapping(method = RequestMethod.POST, value = "/globalLocationMap/build")
    public String buildGLMFromPlans(Model model,
                                    @RequestParam(name = "dryrun", required = false) Boolean dryrun,
                                    @RequestParam(name = "buildSources", required = false) Boolean buildSources,
                                    @RequestParam(name = "partitionLevelMisMatch", required = false) Boolean partitionLevelMisMatch,
                                    @RequestParam(name = "consolidationLevel", required = false) Integer consolidationLevel)
            throws MismatchException, SessionRunningException, RequiredConfigurationException {
        log.info("Building global location maps");
        boolean lclDryrun = dryrun != null ? dryrun : true;

        // Reset Connections and reload most current config.
        executeSessionService.clearActiveSession();
        executeSessionService.transitionLoadedSessionToActive();

        boolean lclBuildSources = buildSources != null ? buildSources : false;
        int lclConsolidationLevel = consolidationLevel != null ? consolidationLevel : 1;
        boolean lclPartitionLevelMismatch = partitionLevelMisMatch != null && partitionLevelMisMatch;

        if (lclBuildSources) {
            WarehouseMapBuilder wmb = databaseService.buildDatabaseSources(lclConsolidationLevel, false);
            model.addAttribute("sources", wmb.getSources());
        }

        Map<String, String> globalLocationMap = translatorService.buildGlobalLocationMapFromWarehousePlansAndSources(lclDryrun, lclConsolidationLevel);

        if (dryrun) {
            model.addAttribute("action", "view.dryrun");
            model.addAttribute("glm", globalLocationMap);
            return "/globalLocationMap/view_edit";
        } else {
            return "redirect:/config/view";
        }
    }

}
