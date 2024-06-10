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
import com.cloudera.utils.hms.mirror.domain.support.ExecuteSession;
import com.cloudera.utils.hms.mirror.exceptions.SessionRunningException;
import com.cloudera.utils.hms.mirror.service.ConfigService;
import com.cloudera.utils.hms.mirror.service.ExecuteSessionService;
import com.cloudera.utils.hms.mirror.util.ModelUtils;
import com.cloudera.utils.hms.mirror.web.service.WebConfigService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Set;
import java.util.UUID;

@Controller
@RequestMapping(path = "/config")
@Slf4j
public class ConfigMVController {

    private ConfigService configService;
    private ExecuteSessionService executeSessionService;
    private WebConfigService webConfigService;

    @Autowired
    public void setConfigService(ConfigService configService) {
        this.configService = configService;
    }

    @Autowired
    public void setExecuteSessionService(ExecuteSessionService executeSessionService) {
        this.executeSessionService = executeSessionService;
    }

    @Autowired
    public void setWebConfigService(WebConfigService webConfigService) {
        this.webConfigService = webConfigService;
    }

    @RequestMapping(value = "/list", method = RequestMethod.GET)
    public String list(Model model) {
        String loadedSessionId = executeSessionService.getLoadedSession().getSessionId();
        List<String> configs = webConfigService.getConfigList();
        model.addAttribute("action", "list");
        model.addAttribute("loadedSessionId", loadedSessionId);
        model.addAttribute("configs", configs);
        return "/config/list";
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

        model.addAttribute("action", "view");
        model.addAttribute("config", config);
        model.addAttribute("sessionId", configId);
        ModelUtils.allEnumsForModel(model);
        return "/config/view";
    }


}
