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

import com.cloudera.utils.hms.mirror.domain.Cluster;
import com.cloudera.utils.hms.mirror.domain.HiveServer2Config;
import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.domain.support.ExecuteSession;
import com.cloudera.utils.hms.mirror.exceptions.SessionRunningException;
import com.cloudera.utils.hms.mirror.service.ExecuteSessionService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import java.io.IOException;

@Controller
@RequestMapping(path = "/hiveServer2")
@Slf4j
public class HiveServer2MVControl {

    private ExecuteSessionService executeSessionService;

    @Autowired
    public void setExecuteSessionService(ExecuteSessionService executeSessionService) {
        this.executeSessionService = executeSessionService;
    }

    @RequestMapping(value = "/edit", method = RequestMethod.GET)
    public String edit(Model model,
                       @RequestParam(value = "environment", required = true) String environment) {
        model.addAttribute("action", "edit");
        ExecuteSession curSession = executeSessionService.getLoadedSession();
        HmsMirrorConfig currentConfig = curSession.getConfig();
        Cluster cluster = currentConfig.getCluster(Environment.valueOf(environment));
        HiveServer2Config hiveServer2Config = cluster.getHiveServer2();
        model.addAttribute("hiveServer2", hiveServer2Config);
        model.addAttribute("environment", Environment.valueOf(environment));
        return "/hiveServer2/edit";
    }


    @RequestMapping(value = "/save", method = RequestMethod.POST)
    public String save(Model model,
                       @Value("${hms-mirror.config.path}") String configPath,
                       @ModelAttribute("environment") Environment environment,
                       @ModelAttribute("hiveServer2") HiveServer2Config hiveServer2) throws IOException, SessionRunningException {
        // Ensure we are in an editable state.
        executeSessionService.clearActiveSession();
        // Get the current session config.
        ExecuteSession curSession = executeSessionService.getLoadedSession();
        HmsMirrorConfig currentConfig = curSession.getConfig();

        Cluster currentCluster = currentConfig.getCluster(environment);

        // Transpose from the current config.
        currentCluster.setHiveServer2(hiveServer2);

        // Reload ui from the view.
        return "redirect:/config/view";
    }


}
