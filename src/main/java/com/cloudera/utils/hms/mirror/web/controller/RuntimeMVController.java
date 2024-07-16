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

import com.cloudera.utils.hms.mirror.domain.support.RunContainer;
import com.cloudera.utils.hms.mirror.domain.support.RunStatus;
import com.cloudera.utils.hms.mirror.exceptions.EncryptionException;
import com.cloudera.utils.hms.mirror.exceptions.MismatchException;
import com.cloudera.utils.hms.mirror.exceptions.RequiredConfigurationException;
import com.cloudera.utils.hms.mirror.exceptions.SessionException;
import com.cloudera.utils.hms.mirror.service.UIModelService;
import com.cloudera.utils.hms.mirror.web.service.RuntimeService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import static com.cloudera.utils.hms.mirror.web.controller.ControllerReferences.RUN_CONTAINER;

@Controller
@RequestMapping(path = "/runtime")
@Slf4j
public class RuntimeMVController {

    private RuntimeService runtimeService;
    private UIModelService uiModelService;

    @Autowired
    public void setRuntimeService(RuntimeService runtimeService) {
        this.runtimeService = runtimeService;
    }

    @Autowired
    public void setUiModelService(UIModelService uiModelService) {
        this.uiModelService = uiModelService;
    }

    @RequestMapping(value = "/start", method = RequestMethod.GET)
    public String start(Model model,
                          @Value("${hms-mirror.concurrency.max-threads}") Integer maxThreads) {
        uiModelService.sessionToModel(model, maxThreads, false);
        return "runtime/index";
    }

    @RequestMapping(value = "/status", method = RequestMethod.GET)
    public String status(Model model,
                        @Value("${hms-mirror.concurrency.max-threads}") Integer maxThreads) {
        uiModelService.sessionToModel(model, maxThreads, false);
        return "runstatus/view";
    }

    @RequestMapping(value = "/reports", method = RequestMethod.GET)
    public String reports(Model model,
                         @Value("${hms-mirror.concurrency.max-threads}") Integer maxThreads) {
        uiModelService.sessionToModel(model, maxThreads, false);
        return "runtime/reports";
    }


    @RequestMapping(method = RequestMethod.POST, value = "/doStart")
    public String doStart(Model model,
            @ModelAttribute(RUN_CONTAINER) RunContainer runContainer,
                        @Value("${hms-mirror.concurrency.max-threads}") Integer maxThreads) throws MismatchException, RequiredConfigurationException, SessionException, EncryptionException {
//        boolean lclAutoGLM = runContainer.isAutoGLM() != null && runContainer.getAutoGLM();
//        boolean lclDryRun = runContainer.isDryrun() != null && runContainer.getDryrun();

        RunStatus runStatus =  runtimeService.start(runContainer.isDryrun(), runContainer.isAutoGLM(), maxThreads);
        runStatus.setConcurrency(maxThreads);
        // Not necessary..  will be fetched in config/home
//        model.addAttribute(RUN_STATUS, runStatus);
        return "redirect:/config/home";
    }


}
