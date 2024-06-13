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

import com.cloudera.utils.hive.config.DBStore;
import com.cloudera.utils.hms.mirror.domain.Cluster;
import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.support.*;
import com.cloudera.utils.hms.mirror.exceptions.SessionRunningException;
import com.cloudera.utils.hms.mirror.service.ConfigService;
import com.cloudera.utils.hms.mirror.service.DatabaseService;
import com.cloudera.utils.hms.mirror.service.ExecuteSessionService;
import com.cloudera.utils.hms.mirror.util.ModelUtils;
import com.cloudera.utils.hms.mirror.web.service.WebConfigService;
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

// [MermaidChart: f619f3bf-1b39-426d-b48e-71b8c80a8524]

@Controller
@RequestMapping(path = "/config")
@Slf4j
public class ConfigMVController {

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
    public String home(Model model) {
        model.addAttribute("action", "home");

        SessionContainer sessionContainer = new SessionContainer();
        sessionContainer.loadFromSession(executeSessionService.getLoadedSession());

        // Get list of available configs
        List<String> configs = webConfigService.getConfigList();
        model.addAttribute("configs", configs);
        model.addAttribute("sessionContainer", sessionContainer);

        // Get list of Reports
        ReportContainer reportContainer = new ReportContainer();
        reportContainer.setReports(executeSessionService.getAvailableReports());
        model.addAttribute("reportContainer", reportContainer);

        ModelUtils.allEnumsForModel(model);

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
    @RequestMapping(value = "/create", method = RequestMethod.GET)
    public String create(Model model) throws SessionRunningException {
        model.addAttribute("action", "create");
        executeSessionService.clearLoadedSession();
        internalUpsert(model, false);
        return "/config/view_edit";
    }

    private void internalUpsert(Model model, boolean readOnly) {
        model.addAttribute("action", "create");
        SessionContainer sessionContainer = new SessionContainer();

        ExecuteSession session = executeSessionService.getLoadedSession();
        HmsMirrorConfig config = null;
        if (session == null) {
            config = new HmsMirrorConfig();

            // Init Storage migration.
            config.setDataStrategy(DataStrategyEnum.STORAGE_MIGRATION);
            Cluster cluster = config.getCluster(Environment.LEFT);
            cluster.setLegacyHive(Boolean.FALSE);
            // Init Metastore Direct
            cluster.setMetastoreDirect(new DBStore());

        } else {
            model.addAttribute("action", "edit");
            config = session.getConfig();
            sessionContainer.setSessionId(session.getSessionId());
        }

        sessionContainer.setConfig(config);
        sessionContainer.setReadOnly(readOnly);
        model.addAttribute("sessionContainer", sessionContainer);

        ModelUtils.allEnumsForModel(model);
    }

    @RequestMapping(value = "/edit", method = RequestMethod.GET)
    public String edit(Model model) throws SessionRunningException {
        executeSessionService.clearActiveSession();
        model.addAttribute("action", "edit");
        internalUpsert(model, false);
        return "/config/view_edit";
    }


    @RequestMapping(value = "/save", method = RequestMethod.POST)
    public String save(Model model,
                       @Value("${hms-mirror.config.path}") String configPath,
                       @ModelAttribute("container") SessionContainer container) throws IOException, SessionRunningException {

        executeSessionService.clearLoadedSession();

        HmsMirrorConfig config = container.getConfig();
        if (container.isFlipConfig()) {
            executeSessionService.flipConfig(config);
        }

        String saveAs = null;
        if (container.isSaveAsDefault()) {
            log.info("Saving Config as Default: {}", "default.yaml");
            saveAs = "default.yaml";
            String configFullFilename = configPath + File.separator + saveAs;
            configService.saveConfig(config, configFullFilename, Boolean.TRUE);
        }
        if (container.getSessionId() != null) {
            saveAs = container.getSessionId();
            if (!(saveAs.contains(".yaml") || saveAs.contains(".yml"))) {
                saveAs += ".yaml";
            }
            log.info("Saving Config as: {}", saveAs);
            String configFullFilename = configPath + File.separator + saveAs;
            configService.saveConfig(config, configFullFilename, Boolean.TRUE);
        }

        ExecuteSession session = executeSessionService.createSession(saveAs, config);
        // Set it as the current session.
        executeSessionService.setLoadedSession(session);
//        executeSessionService.transitionLoadedSessionToActive();

        internalUpsert(model, true);
        model.addAttribute("action", "view");

        ModelUtils.allEnumsForModel(model);
        return "/config/view_edit";
    }

    @RequestMapping(value = "/reload", method = RequestMethod.POST)
    public String reload(Model model,
                         @RequestParam(value = "config_id", required = true) String configId) throws SessionRunningException {

        // Don't reload if running.
        executeSessionService.clearActiveSession();

        log.info("ReLoading Config: {}", configId);
        HmsMirrorConfig config = configService.loadConfig(configId);
        // Remove the old session
        executeSessionService.getSessions().remove(configId);
        // Create a new session
        ExecuteSession session = executeSessionService.createSession(configId, config);

        // Set it as the current session.
        executeSessionService.setLoadedSession(session);
        internalUpsert(model, true);
        model.addAttribute("action", "view");
//        model.addAttribute("config", config);
//        model.addAttribute("sessionId", configId);
        ModelUtils.allEnumsForModel(model);
        return "/config/view_edit";
    }

    @RequestMapping(value = "/view", method = RequestMethod.GET)
    public String view(Model model) throws SessionRunningException {
        model.addAttribute("action", "view");
        internalUpsert(model, true);
//        model.addAttribute("readonly", true);
        return "/config/view_edit";
    }

    @RequestMapping(value = "/warehouse/plan/add", method = RequestMethod.POST)
    public String addDatabase(Model model,
                              @RequestParam(value = "database", required = true) String database,
                              @RequestParam(value = "externalDirectory", required = true) String externalDirectory,
                              @RequestParam(value = "managedDirectory", required = true) String managedDirectory
                              ) throws SessionRunningException {
        // Don't reload if running.
        executeSessionService.clearActiveSession();

//        SessionContainer container = (SessionContainer)model.getAttribute("sessionContainer");
//        assert container != null;
//        String database = container.getDatabase();
//        String externalLocation = container.getWarehouse().getExternalDirectory();
//        String managedLocation = container.getWarehouse().getManagedDirectory();

        log.info("Adding Warehouse Plan: {} E:{} M:{}", database, externalDirectory, managedDirectory);

        databaseService.addWarehousePlan(database, externalDirectory, managedDirectory);


//        HmsMirrorConfig config = configService.loadConfig(configId);
//        // Remove the old session
//        executeSessionService.getSessions().remove(configId);
//        // Create a new session
//        ExecuteSession session = executeSessionService.createSession(configId, config);
//
//        ExecuteSession session = executeSessionService.getActiveSession();
//        // Set it as the current session.
//        HmsMirrorConfig config = session.getResolvedConfig();

        model.addAttribute("action", "view");
        internalUpsert(model, true);

//        model.addAttribute("config", config);
//        model.addAttribute("sessionId", session.getSessionId());
//        ModelUtils.allEnumsForModel(model);
        return "/config/view_edit";
    }


}
