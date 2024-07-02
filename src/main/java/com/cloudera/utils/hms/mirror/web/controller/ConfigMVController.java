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
import com.cloudera.utils.hms.mirror.service.PasswordService;
import com.cloudera.utils.hms.mirror.util.ModelUtils;
import com.cloudera.utils.hms.mirror.web.service.WebConfigService;
import com.jcabi.manifests.Manifests;
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
import java.util.Set;

import static com.cloudera.utils.hms.mirror.cli.HmsMirrorCommandLineOptionsEnum.PASSWORD_KEY;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

@Controller
@RequestMapping(path = "/config")
@Slf4j
public class ConfigMVController implements ControllerReferences {

    private ConfigService configService;
    private ExecuteSessionService executeSessionService;
    private PasswordService passwordService;
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
    public void setPasswordService(PasswordService passwordService) {
        this.passwordService = passwordService;
    }

    @Autowired
    public void setWebConfigService(WebConfigService webConfigService) {
        this.webConfigService = webConfigService;
    }

    @RequestMapping(value = "/home", method = RequestMethod.GET)
    public String home(Model model,
                       @Value("${hms-mirror.config.testing}") Boolean testing,
                       @Value("${hms-mirror.concurrency.max-threads}") Integer maxThreads) {
        model.addAttribute(ACTION, "home");

//        ExecuteSession executeSession = executeSessionService.getLoadedSession();
        model.addAttribute(CONCURRENCY, maxThreads);

        // Get list of available configs
        Set<String> configs = webConfigService.getConfigList();
        model.addAttribute(CONFIG_LIST, configs);

        sessionToModel(model, maxThreads, testing);

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
                         @RequestParam(value = DATA_STRATEGY, required = true) String dataStrategy,
                         @Value("${hms-mirror.concurrency.max-threads}") Integer maxThreads) throws SessionException {
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

        sessionToModel(model, maxThreads, Boolean.FALSE);

        return "/config/view";
    }

    private void sessionToModel(Model model, Integer concurrency, Boolean testing) {


        // This is so we can try to load a list of available databases.
        try {
            log.info("Attempting connection to get list of databases.");
            executeSessionService.transitionLoadedSessionToActive(concurrency);
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
                runStatus.setConcurrency(concurrency);
                runStatus.addError(MessageCode.RESET_TO_DEFAULT_LOCATION_WITHOUT_WAREHOUSE_DIRS);
                runStatus.addWarning(MessageCode.RESET_TO_DEFAULT_LOCATION);
                model.addAttribute(RUN_STATUS, runStatus);
            } else {
                RunStatus runStatus = session.getRunStatus();
                runStatus.setConcurrency(concurrency);
                model.addAttribute(RUN_STATUS, runStatus);
            }
        }
        model.addAttribute(RUN_CONTAINER, runContainer);
        ModelUtils.allEnumsForModel(model);
    }

    @RequestMapping(value = "/edit", method = RequestMethod.GET)
    public String edit(Model model,
                       @Value("${hms-mirror.concurrency.max-threads}") Integer maxThreads) throws SessionException {
        executeSessionService.clearActiveSession();
        model.addAttribute(ACTION, "edit");
        model.addAttribute(READ_ONLY, Boolean.FALSE);
        sessionToModel(model, maxThreads, Boolean.FALSE);
        return "/config/view";
    }

    @RequestMapping(value = "/save", method = RequestMethod.POST)
    public String save(Model model,
                       @ModelAttribute(CONFIG) HmsMirrorConfig config,
                       @Value("${hms-mirror.concurrency.max-threads}") Integer maxThreads) throws SessionException {
        executeSessionService.clearActiveSession();
        model.addAttribute(ACTION, "view");

        ExecuteSession session = executeSessionService.getLoadedSession();
        HmsMirrorConfig currentConfig = session.getConfig();

        // Merge Passwords
        config.getClusters().forEach((env, cluster) -> {
            // HS2
            if (nonNull(cluster.getHiveServer2())) {
                String currentPassword = (String) currentConfig.getClusters().get(env).getHiveServer2().getConnectionProperties().get("password");
                String newPassword = (String) cluster.getHiveServer2().getConnectionProperties().get("password");
                if (newPassword != null && !newPassword.isEmpty()) {
                    // Set new Password
                    cluster.getHiveServer2().getConnectionProperties().put("password", newPassword);
                } else {
                    // Restore original password
                    cluster.getHiveServer2().getConnectionProperties().put("password", currentPassword);
                }
            }

            // Metastore
            if (nonNull(cluster.getMetastoreDirect())) {
                String currentPassword = (String) currentConfig.getClusters().get(env).getMetastoreDirect().getConnectionProperties().get("password");
                String newPassword = (String)cluster.getMetastoreDirect().getConnectionProperties().get("password");
                if (newPassword != null && !newPassword.isEmpty()) {
                    // Set new password
                    cluster.getMetastoreDirect().getConnectionProperties().put("password", newPassword);
                } else {
                    // Restore Original password
                    cluster.getMetastoreDirect().getConnectionProperties().put("password", currentPassword);
                }
            }
        });

        // Merge
//        currentConfig.getClusters().forEach((env, cluster) -> {
//            config.getClusters().put(env, cluster);
//        });
        config.setTranslator(currentConfig.getTranslator());

        // Reset to the merged config.
        session.setConfig(config);
        model.addAttribute(READ_ONLY, Boolean.TRUE);
        sessionToModel(model, maxThreads, Boolean.FALSE);
        return "/config/view";
    }


    // TODO: Need to adjust this to a persist only. Adjust SessionContainer (drop) and replace with
    //      GET path variables to control how to save.
    @RequestMapping(value = "/persist", method = RequestMethod.POST)
    public String persist(Model model,
                          @Value("${hms-mirror.config.path}") String configPath,
                          @ModelAttribute(PERSIST) PersistContainer persistContainer,
                          @Value("${hms-mirror.concurrency.max-threads}") Integer maxThreads) throws IOException, SessionException {
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

        sessionToModel(model, maxThreads, Boolean.FALSE);
        model.addAttribute(ACTION, "view");

        ModelUtils.allEnumsForModel(model);
        return "/config/view";
    }

    @RequestMapping(value = "/reload", method = RequestMethod.POST)
    public String reload(Model model,
                         @RequestParam(value = SESSION_ID, required = true) String sessionId,
                         @Value("${hms-mirror.concurrency.max-threads}") Integer maxThreads) throws SessionException {
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
        sessionToModel(model, maxThreads, Boolean.FALSE);
        model.addAttribute(ACTION, "view");
        model.addAttribute(READ_ONLY, Boolean.FALSE);

        ModelUtils.allEnumsForModel(model);
        return "/config/view";
    }

    @RequestMapping(value = "/view", method = RequestMethod.GET)
    public String view(Model model,
                       @Value("${hms-mirror.concurrency.max-threads}") Integer maxThreads) throws SessionException {
        model.addAttribute(ACTION, "view");
        model.addAttribute(READ_ONLY, Boolean.TRUE);
        sessionToModel(model, maxThreads, Boolean.FALSE);
        return "/config/view";
    }

    @RequestMapping(value = "/encryptPasswords", method = RequestMethod.POST)
    public String encryptPasswords(Model model,
                                   @RequestParam(value = PASSWORD_KEY, required = true) String passwordKey) throws SessionException {
        executeSessionService.clearActiveSession();
        HmsMirrorConfig config = executeSessionService.getLoadedSession().getConfig();
        String lhs2 = null;
        String lms = null;
        String rhs2 = null;
        String rms = null;
        if (!config.isEncryptedPasswords()) {
            if (nonNull(config.getCluster(Environment.LEFT)) && nonNull(config.getCluster(Environment.LEFT).getHiveServer2())) {
                lhs2 = passwordService.encryptPassword(passwordKey, config.getCluster(Environment.LEFT).getHiveServer2().getConnectionProperties().getProperty("password"));
            }
            if (nonNull(config.getCluster(Environment.RIGHT)) && nonNull(config.getCluster(Environment.RIGHT).getHiveServer2())) {
                rhs2 = passwordService.encryptPassword(passwordKey, config.getCluster(Environment.RIGHT).getHiveServer2().getConnectionProperties().getProperty("password"));
            }
            if (nonNull(config.getCluster(Environment.LEFT)) && nonNull(config.getCluster(Environment.LEFT).getMetastoreDirect())) {
                lms = passwordService.encryptPassword(passwordKey, config.getCluster(Environment.LEFT).getMetastoreDirect().getConnectionProperties().getProperty("password"));
            }
            if (nonNull(config.getCluster(Environment.RIGHT)) && nonNull(config.getCluster(Environment.RIGHT).getMetastoreDirect())) {
                rms = passwordService.encryptPassword(passwordKey, config.getCluster(Environment.RIGHT).getMetastoreDirect().getConnectionProperties().getProperty("password"));
            }
            config.setEncryptedPasswords(Boolean.TRUE);
            if (nonNull(lhs2)) {
                config.getCluster(Environment.LEFT).getHiveServer2().getConnectionProperties().setProperty("password", lhs2);
            }
            if (nonNull(rhs2)) {
                config.getCluster(Environment.RIGHT).getHiveServer2().getConnectionProperties().setProperty("password", rhs2);
            }
            if (nonNull(lms)) {
                config.getCluster(Environment.LEFT).getMetastoreDirect().getConnectionProperties().setProperty("password", lms);
            }
            if (nonNull(rms)) {
                config.getCluster(Environment.RIGHT).getMetastoreDirect().getConnectionProperties().setProperty("password", rms);
            }
        }
//        model.addAttribute(ACTION, "view");
//        model.addAttribute(READ_ONLY, Boolean.TRUE);
//        sessionToModel(model, maxThreads, Boolean.FALSE);
        return "redirect:/config/view";
    }

    @RequestMapping(value = "/decryptPasswords", method = RequestMethod.POST)
    public String decryptPasswords(Model model,
                                   @RequestParam(value = PASSWORD_KEY, required = true) String passwordKey) throws SessionException {
        executeSessionService.clearActiveSession();
        HmsMirrorConfig config = executeSessionService.getLoadedSession().getConfig();
        String lhs2 = null;
        String lms = null;
        String rhs2 = null;
        String rms = null;
        if (config.isEncryptedPasswords()) {
            if (nonNull(config.getCluster(Environment.LEFT)) && nonNull(config.getCluster(Environment.LEFT).getHiveServer2())) {
                lhs2 = passwordService.decryptPassword(passwordKey, config.getCluster(Environment.LEFT).getHiveServer2().getConnectionProperties().getProperty("password"));
            }
            if (nonNull(config.getCluster(Environment.RIGHT)) && nonNull(config.getCluster(Environment.RIGHT).getHiveServer2())) {
                rhs2 = passwordService.decryptPassword(passwordKey, config.getCluster(Environment.RIGHT).getHiveServer2().getConnectionProperties().getProperty("password"));
            }
            if (nonNull(config.getCluster(Environment.LEFT)) && nonNull(config.getCluster(Environment.LEFT).getMetastoreDirect())) {
                lms = passwordService.decryptPassword(passwordKey, config.getCluster(Environment.LEFT).getMetastoreDirect().getConnectionProperties().getProperty("password"));
            }
            if (nonNull(config.getCluster(Environment.RIGHT)) && nonNull(config.getCluster(Environment.RIGHT).getMetastoreDirect())) {
                rms = passwordService.decryptPassword(passwordKey, config.getCluster(Environment.RIGHT).getMetastoreDirect().getConnectionProperties().getProperty("password"));
            }
            config.setEncryptedPasswords(Boolean.FALSE);
            if (nonNull(lhs2)) {
                config.getCluster(Environment.LEFT).getHiveServer2().getConnectionProperties().setProperty("password", lhs2);
            }
            if (nonNull(rhs2)) {
                config.getCluster(Environment.RIGHT).getHiveServer2().getConnectionProperties().setProperty("password", rhs2);
            }
            if (nonNull(lms)) {
                config.getCluster(Environment.LEFT).getMetastoreDirect().getConnectionProperties().setProperty("password", lms);
            }
            if (nonNull(rms)) {
                config.getCluster(Environment.RIGHT).getMetastoreDirect().getConnectionProperties().setProperty("password", rms);
            }
        }
//        model.addAttribute(ACTION, "view");
//        model.addAttribute(READ_ONLY, Boolean.TRUE);
//        sessionToModel(model, maxThreads, Boolean.FALSE);
        return "redirect:/config/view";
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