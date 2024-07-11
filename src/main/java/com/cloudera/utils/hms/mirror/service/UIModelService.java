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

package com.cloudera.utils.hms.mirror.service;

import com.cloudera.utils.hms.mirror.MessageCode;
import com.cloudera.utils.hms.mirror.domain.support.ExecuteSession;
import com.cloudera.utils.hms.mirror.domain.support.PersistContainer;
import com.cloudera.utils.hms.mirror.domain.support.RunContainer;
import com.cloudera.utils.hms.mirror.domain.support.RunStatus;
import com.cloudera.utils.hms.mirror.util.ModelUtils;
import com.cloudera.utils.hms.mirror.web.controller.ControllerReferences;
import com.jcabi.manifests.Manifests;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.ui.Model;

import static java.util.Objects.isNull;

@Service
@Getter
@Setter
@Slf4j
public class UIModelService implements ControllerReferences {

    private ExecuteSessionService executeSessionService;

    @Autowired
    public void setExecuteSessionService(ExecuteSessionService executeSessionService) {
        this.executeSessionService = executeSessionService;
    }

    public void sessionToModel(Model model, Integer concurrency, Boolean testing) {

        boolean lclTesting = testing != null && testing;

        model.addAttribute(CONCURRENCY, concurrency);

        ExecuteSession session = executeSessionService.getSession();

        RunContainer runContainer = new RunContainer();
        model.addAttribute(RUN_CONTAINER, runContainer);
        if (session != null) {
            runContainer.setSessionId(session.getSessionId());

            model.addAttribute(CONFIG, session.getConfig());

            PersistContainer persistContainer = new PersistContainer();
            persistContainer.setSaveAs(session.getSessionId());
            model.addAttribute(PERSIST, persistContainer);

            // For testing only.
            if (lclTesting && isNull(session.getRunStatus())) {
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

        try {
            model.addAttribute(VERSION, Manifests.read("HMS-Mirror-Version"));
        } catch (IllegalArgumentException iae) {
            model.addAttribute(VERSION, "Unknown");
        }

        ModelUtils.allEnumsForModel(model);
    }

}
