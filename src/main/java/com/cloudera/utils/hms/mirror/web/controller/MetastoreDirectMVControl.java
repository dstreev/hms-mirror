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
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.domain.support.ExecuteSession;
import com.cloudera.utils.hms.mirror.exceptions.SessionException;
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
@RequestMapping(path = "/metastoreDirect")
@Slf4j
public class MetastoreDirectMVControl {

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
        DBStore metastore = cluster.getMetastoreDirect();
        model.addAttribute("metastoreDirect", metastore);
        model.addAttribute("environment", Environment.valueOf(environment));
        return "/metastoreDirect/edit";
    }


    @RequestMapping(value = "/save", method = RequestMethod.POST)
    public String save(Model model,
                       @Value("${hms-mirror.config.path}") String configPath,
                       @ModelAttribute("environment") Environment environment,
                       @ModelAttribute("metastoreDirect") DBStore metastoreDirect) throws IOException, SessionException {
        // Ensure we are in an editable state.
        executeSessionService.clearActiveSession();
        // Get the current session config.
        ExecuteSession curSession = executeSessionService.getLoadedSession();
        HmsMirrorConfig currentConfig = curSession.getConfig();

        Cluster currentCluster = currentConfig.getCluster(environment);

        // Transpose from the current config.
        currentCluster.setMetastoreDirect(metastoreDirect);

        // Reload ui from the view.
        return "redirect:/config/view";
    }


}
