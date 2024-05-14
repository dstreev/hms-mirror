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

package com.cloudera.utils.hms.mirror.web.controller.api.v1.runtime;

import com.cloudera.utils.hms.mirror.domain.support.RunStatus;
import com.cloudera.utils.hms.mirror.service.ConfigService;
import com.cloudera.utils.hms.mirror.service.ConnectionPoolService;
import com.cloudera.utils.hms.mirror.service.ExecuteSessionService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@Slf4j
@RequestMapping(path = "/api/v1/runtime")
public class RuntimeController {

    private ConfigService configService;
    private ExecuteSessionService executeSessionService;
    private ConnectionPoolService connectionPoolService;

    @Autowired
    public void setConfigService(ConfigService ConfigService) {
        this.configService = ConfigService;
    }

    @Autowired
    public void setHmsMirrorCfgService(ExecuteSessionService executeSessionService) {
        this.executeSessionService = executeSessionService;
    }

    @Autowired
    public void setConnectionPoolService(ConnectionPoolService connectionPoolService) {
        this.connectionPoolService = connectionPoolService;
    }


//    public RunStatus validate(String sessionId) {
//        configService.validate(sessionId);
//
//        return executeSessionService.getSession(sessionId).getRunStatus();
//    }


    public RunStatus start(String sessionId, boolean dryrun) {
        executeSessionService.getSession(sessionId).getHmsMirrorConfig().setExecute(!dryrun);

        /////  Should we consider moving this to the RuntimeService?  /////

        // Close all connections, so we can ensure we have a clean start.
        connectionPoolService.close();

        if (configService.validate()) {
            // TODO: Execute the session.
//            executeSessionService.start(sessionId);
        } else {
            log.error("Validation failed. Exiting.");
        }

        return executeSessionService.getSession(sessionId).getRunStatus();
    }

    public RunStatus status(String sessionId) {
        return executeSessionService.getSession(sessionId).getRunStatus();
    }

    public RunStatus stop(String sessionId) {
        // TODO: Stop a running session.

        return executeSessionService.getSession(sessionId).getRunStatus();
    }

}
