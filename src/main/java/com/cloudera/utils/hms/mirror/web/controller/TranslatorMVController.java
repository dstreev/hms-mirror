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

import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.WarehouseMapBuilder;
import com.cloudera.utils.hms.mirror.exceptions.EncryptionException;
import com.cloudera.utils.hms.mirror.exceptions.MismatchException;
import com.cloudera.utils.hms.mirror.exceptions.RequiredConfigurationException;
import com.cloudera.utils.hms.mirror.exceptions.SessionException;
import com.cloudera.utils.hms.mirror.service.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import javax.validation.constraints.NotNull;
import java.util.Map;

import static com.cloudera.utils.hms.mirror.web.controller.ControllerReferences.*;

@Controller
@RequestMapping(path = "/translator")
@Slf4j
public class TranslatorMVController {

    private DatabaseService databaseService;
    private ExecuteSessionService executeSessionService;
    private TranslatorService translatorService;
    private ConfigService configService;
    private UIModelService uiModelService;

    @Autowired
    public void setConfigService(ConfigService configService) {
        this.configService = configService;
    }

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

    @Autowired
    public void setUiModelService(UIModelService uiModelService) {
        this.uiModelService = uiModelService;
    }

    @RequestMapping(value = "/globalLocationMap/add", method = RequestMethod.POST)
    public String addGlobalLocationMap(@RequestParam(name = SOURCE, required = true) String source,
                                       @RequestParam(name = TARGET, required = true) String target) throws SessionException {
        log.info("Adding global location map for source: {} and target: {}", source, target);
        // Don't reload if running.
        executeSessionService.clearActiveSession();

        translatorService.addGlobalLocationMap(source, target);

        return "redirect:/config/view";
    }

    @RequestMapping(value = "/globalLocationMap/{source}/delete", method = RequestMethod.GET)
    public String removeGlobalLocationMap(Model model,
                                          @PathVariable @NotNull String source) throws SessionException {
        // Don't reload if running.
        executeSessionService.clearActiveSession();

        log.info("Removing global location map for source: {}", source);
        translatorService.removeGlobalLocationMap(source);

        return "redirect:/config/view";
    }

    @RequestMapping(method = RequestMethod.POST, value = "/globalLocationMap/build")
    public String buildGLMFromPlans(Model model,
                                    @RequestParam(name = GLM_DRYRUN, required = false) Boolean dryrun,
//                                    @RequestParam(name = BUILD_SOURCES, required = false) Boolean buildSources,
                                    @RequestParam(name = PARTITION_LEVEL_MISMATCH, required = false) Boolean partitionLevelMisMatch,
                                    @RequestParam(name = CONSOLIDATION_LEVEL, required = false) Integer consolidationLevel,
                                    @Value("${hms-mirror.concurrency.max-threads}") Integer maxThreads)
            throws MismatchException, SessionException, RequiredConfigurationException, EncryptionException {
        log.info("Building global location maps");
        boolean lclDryrun = dryrun != null ? dryrun : false;

        // Reset Connections and reload most current config.
        executeSessionService.clearActiveSession();
        if (executeSessionService.transitionLoadedSessionToActive(maxThreads, Boolean.TRUE)) {

//            boolean lclBuildSources = buildSources != null ? buildSources : false;
            int lclConsolidationLevel = consolidationLevel != null ? consolidationLevel : 1;
            boolean lclPartitionLevelMismatch = partitionLevelMisMatch != null && partitionLevelMisMatch;

//            if (lclBuildSources) {
            WarehouseMapBuilder wmb = databaseService.buildDatabaseSources(lclConsolidationLevel, false);
            model.addAttribute(SOURCES, wmb.getSources());
//            }

            Map<String, String> globalLocationMap = translatorService.buildGlobalLocationMapFromWarehousePlansAndSources(lclDryrun, lclConsolidationLevel);

            if (lclDryrun) {
//                model.addAttribute(ACTION, "view.dryrun");
                HmsMirrorConfig lclConfig = new HmsMirrorConfig();
                lclConfig.getTranslator().setOrderedGlobalLocationMap(globalLocationMap);
                lclConfig.getTranslator().setWarehouseMapBuilder(wmb);
                model.addAttribute(CONFIG, lclConfig);
                return "translator/globalLocationMap/view";
            } else {
                HmsMirrorConfig config = executeSessionService.getSession().getConfig();
                config.getTranslator().setWarehouseMapBuilder(wmb);
                config.getTranslator().setGlobalLocationMap(globalLocationMap);
                configService.validate(executeSessionService.getSession(), null, Boolean.FALSE);
                return "redirect:/config/view";
            }
        } else {
            uiModelService.sessionToModel(model, 1, Boolean.FALSE);
            model.addAttribute(TYPE, "Connections");
            model.addAttribute(MESSAGE, "Issue validating connections.  Review Messages and try again.");
            return "error";
        }
    }

}
