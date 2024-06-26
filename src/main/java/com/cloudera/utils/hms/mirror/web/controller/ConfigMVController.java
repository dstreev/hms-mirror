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

import com.cloudera.utils.hms.mirror.MessageCode;
import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.Translator;
import com.cloudera.utils.hms.mirror.domain.support.*;
import com.cloudera.utils.hms.mirror.exceptions.SessionException;
import com.cloudera.utils.hms.mirror.service.ConfigService;
import com.cloudera.utils.hms.mirror.service.DatabaseService;
import com.cloudera.utils.hms.mirror.service.ExecuteSessionService;
import com.cloudera.utils.hms.mirror.util.ModelUtils;
import com.cloudera.utils.hms.mirror.web.service.WebConfigService;
import com.jcabi.manifests.Manifests;
import com.jcraft.jsch.Session;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static java.util.Objects.isNull;

@Controller
@RequestMapping(path = "/config")
@Slf4j
public class ConfigMVController implements ControllerReferences {

    private ConfigService configService;
    private ExecuteSessionService executeSessionService;
    private WebConfigService webConfigService;
    private DatabaseService databaseService;

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
    public void setWebConfigService(WebConfigService webConfigService) {
        this.webConfigService = webConfigService;
    }

    @RequestMapping(value = "/home", method = RequestMethod.GET)
    public String home(Model model,
                       @Value("${hms-mirror.config.testing}") Boolean testing) {
        model.addAttribute(ACTION, "home");

//        ExecuteSession executeSession = executeSessionService.getLoadedSession();

        // Get list of available configs
        List<String> configs = webConfigService.getConfigList();
        model.addAttribute(CONFIG_LIST, configs);

        sessionToModel(model, testing);

        // Get list of Reports
        model.addAttribute(REPORT_LIST, executeSessionService.getAvailableReports());
        try {
            model.addAttribute(VERSION, Manifests.read("HMS-Mirror-Version"));
        } catch (IllegalArgumentException iae) {
            model.addAttribute(VERSION, "Unknown");
        }

        return "/config/home";
    }


    //    @RequestMapping(value = "/list", method = RequestMethod.GET)
//    public String list(Model model) {
//        String loadedSessionId = executeSessionService.getLoadedSession().getSessionId();
//        List<String> configs = webConfigService.getConfigList();
//        model.addAttribute("action", "list");
//        model.addAttribute("loadedSessionId", loadedSessionId);
//        model.addAttribute("configs", configs);
//        return "/config/list";
//    }
//
    @RequestMapping(value = "/create", method = RequestMethod.POST)
    public String create(Model model,
                         @RequestParam(value = DATA_STRATEGY, required = true) String dataStrategy) throws SessionException {
        model.addAttribute(ACTION, "create");
        // Clear the loaded and active session.
        executeSessionService.clearLoadedSession();
        // Create new Session (with blank config)
        HmsMirrorConfig config = configService.createForDataStrategy(DataStrategyEnum.valueOf(dataStrategy));
        ExecuteSession session = executeSessionService.createSession(NEW_CONFIG, config);
        // Set the Loaded Session
        executeSessionService.setLoadedSession(session);
//        // Setup Model for MVC
//        model.addAttribute(CONFIG, session.getConfig());
//        model.addAttribute(SESSION_ID, session.getSessionId());

        sessionToModel(model, Boolean.FALSE);

        return "/config/view";
    }

    private void sessionToModel(Model model, Boolean testing) {


        // This is so we can try to load a list of available databases.
        try {
            log.info("Attempting connection to get list of databases.");
            executeSessionService.transitionLoadedSessionToActive();
        } catch (SessionException e) {
            log.warn("Error transitioning to active session: {}", e.getMessage());
        }

//        List<String> availableDatabases = databaseService.listAvailableDatabases(Environment.LEFT);
//        model.addAttribute(AVAILABLE_DATABASES, availableDatabases);

        ExecuteSession session = executeSessionService.getActiveSession();

        RunContainer runContainer = new RunContainer();
        model.addAttribute(RUN_CONTAINER, runContainer);
        if (session != null) {
            runContainer.setSessionId(session.getSessionId());
            runContainer.setDataStrategy(session.getConfig().getDataStrategy());

            model.addAttribute(CONFIG, session.getConfig());

            runContainer.setSaveAs(session.getSessionId());
            // For testing only.
            if (testing && isNull(session.getRunStatus())) {
                RunStatus runStatus = new RunStatus();
                runStatus.addError(MessageCode.RESET_TO_DEFAULT_LOCATION_WITHOUT_WAREHOUSE_DIRS);
                runStatus.addWarning(MessageCode.RESET_TO_DEFAULT_LOCATION);
                model.addAttribute(RUN_STATUS, runStatus);
            } else {
                model.addAttribute(RUN_STATUS, session.getRunStatus());
            }
        }
        model.addAttribute(RUN_CONTAINER, runContainer);
        ModelUtils.allEnumsForModel(model);
    }

    @RequestMapping(value = "/edit", method = RequestMethod.GET)
    public String edit(Model model) throws SessionException {
        executeSessionService.clearActiveSession();
        model.addAttribute(ACTION, "edit");
        model.addAttribute(READ_ONLY, Boolean.FALSE);
        sessionToModel(model, Boolean.FALSE);
        return "/config/view";
    }

    @RequestMapping(value = "/save", method = RequestMethod.POST)
    public String save(Model model,
                       @ModelAttribute(CONFIG) HmsMirrorConfig config) throws SessionException {
        executeSessionService.clearActiveSession();
        model.addAttribute(ACTION, "view");

        ExecuteSession session = executeSessionService.getLoadedSession();
        HmsMirrorConfig currentConfig = session.getConfig();

        // Merge
        currentConfig.getClusters().forEach((env, cluster) -> {
            config.getClusters().put(env, cluster);
        });
        config.setTranslator(currentConfig.getTranslator());

        // Reset to the merged config.
        session.setConfig(config);
        model.addAttribute(READ_ONLY, Boolean.TRUE);
        sessionToModel(model, Boolean.FALSE);
        return "/config/view";
    }


    // TODO: Need to adjust this to a persist only. Adjust SessionContainer (drop) and replace with
    //      GET path variables to control how to save.
    @RequestMapping(value = "/persist", method = RequestMethod.POST)
    public String persist(Model model,
                          @Value("${hms-mirror.config.path}") String configPath,
                          @ModelAttribute(PERSIST) PersistContainer persistContainer) throws IOException, SessionException {
        // Get the current session config.
        ExecuteSession curSession = executeSessionService.getLoadedSession();
        HmsMirrorConfig currentConfig = curSession.getConfig();

        if (persistContainer.isFlipConfigs()) {
            configService.flipConfig(currentConfig);
        }

        // Clone and save clone
        HmsMirrorConfig config = currentConfig.clone();
        if (persistContainer.isStripMappings()) {
            config.setTranslator(new Translator());
        }

        String saveAs = null;
        if (persistContainer.isSaveAsDefault()) {
            log.info("Saving Config as Default: {}", DEFAULT_CONFIG);
            saveAs = DEFAULT_CONFIG;
            String configFullFilename = configPath + File.separator + saveAs;
            configService.saveConfig(config, configFullFilename, Boolean.TRUE);
        }
        if (!persistContainer.getSaveAs().isEmpty()) {
            saveAs = curSession.getSessionId();
            if (!(persistContainer.getSaveAs().contains(".yaml") || persistContainer.getSaveAs().contains(".yml"))) {
                persistContainer.setSaveAs(persistContainer.getSaveAs() + ".yaml");//saveAs += ".yaml";
            }
            log.info("Saving Config as: {}", saveAs);
            String configFullFilename = configPath + File.separator + persistContainer.getSaveAs();
            configService.saveConfig(config, configFullFilename, Boolean.TRUE);
        }

        sessionToModel(model, Boolean.FALSE);
        model.addAttribute(ACTION, "view");

        ModelUtils.allEnumsForModel(model);
        return "/config/view";
    }

    @RequestMapping(value = "/reload", method = RequestMethod.POST)
    public String reload(Model model,
                         @RequestParam(value = SESSION_ID, required = true) String sessionId) throws SessionException {
        // Don't reload if running.
        executeSessionService.clearActiveSession();

        log.info("ReLoading Config: {}", sessionId);
        HmsMirrorConfig config = configService.loadConfig(sessionId);
        // Remove the old session
        executeSessionService.getSessions().remove(sessionId);
        // Create a new session
        ExecuteSession session = executeSessionService.createSession(sessionId, config);
        executeSessionService.setLoadedSession(session);

        // Set it as the current session.
        sessionToModel(model, Boolean.FALSE);
        model.addAttribute(ACTION, "view");
        model.addAttribute(READ_ONLY, Boolean.FALSE);

        ModelUtils.allEnumsForModel(model);
        return "/config/view";
    }

    @RequestMapping(value = "/view", method = RequestMethod.GET)
    public String view(Model model) throws SessionException {
        model.addAttribute(ACTION, "view");
        model.addAttribute(READ_ONLY, Boolean.TRUE);
        sessionToModel(model, Boolean.FALSE);
        return "/config/view";
    }

//    @RequestMapping(value = "/cluster/init", method = RequestMethod.GET)
//    public String initCluster(Model model,
//            @RequestParam(value = ENVIRONMENT, required = true) String environment) throws SessionException {
//        // Don't reload if running.
//        executeSessionService.clearActiveSession();
//
//        Environment env = Environment.valueOf(environment);
//        ExecuteSession session = executeSessionService.getLoadedSession();
//        HmsMirrorConfig config = session.getConfig();
//        config.initClusterFor(env);
//        sessionToModel(model);
//
//        return "/config/view";
//    }
}