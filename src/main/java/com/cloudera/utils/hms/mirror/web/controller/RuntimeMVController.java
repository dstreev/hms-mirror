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
import com.cloudera.utils.hms.mirror.exceptions.MismatchException;
import com.cloudera.utils.hms.mirror.exceptions.RequiredConfigurationException;
import com.cloudera.utils.hms.mirror.exceptions.SessionRunningException;
import com.cloudera.utils.hms.mirror.web.service.RuntimeService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import static com.cloudera.utils.hms.mirror.web.controller.ControllerReferences.RUN_CONTAINER;
import static com.cloudera.utils.hms.mirror.web.controller.ControllerReferences.RUN_STATUS;

@Controller
@RequestMapping(path = "/runtime")
@Slf4j
public class RuntimeMVController {

    private RuntimeService runtimeService;

    @Autowired
    public void setRuntimeService(RuntimeService runtimeService) {
        this.runtimeService = runtimeService;
    }

    @RequestMapping(method = RequestMethod.POST, value = "/start")
    public String start(Model model,
            @ModelAttribute(RUN_CONTAINER) RunContainer runContainer) throws MismatchException, RequiredConfigurationException, SessionRunningException {
        boolean lclAutoGLM = runContainer.getAutoGLM() != null && runContainer.getAutoGLM();
        RunStatus runStatus =  runtimeService.start(runContainer.getDryrun(), lclAutoGLM);
        // Not necessary..  will be fetched in config/home
        model.addAttribute(RUN_STATUS, runStatus);
        return "/config/home";
    }


}
