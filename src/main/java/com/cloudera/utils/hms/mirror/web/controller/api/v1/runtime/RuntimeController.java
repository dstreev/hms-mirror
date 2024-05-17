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

import com.cloudera.utils.hms.mirror.domain.support.ExecuteSession;
import com.cloudera.utils.hms.mirror.domain.support.ProgressEnum;
import com.cloudera.utils.hms.mirror.domain.support.RunStatus;
import com.cloudera.utils.hms.mirror.service.ConfigService;
import com.cloudera.utils.hms.mirror.service.ConnectionPoolService;
import com.cloudera.utils.hms.mirror.service.ExecuteSessionService;
import com.cloudera.utils.hms.mirror.service.HMSMirrorAppService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.Future;

@RestController
@Slf4j
@RequestMapping(path = "/api/v1/runtime")
public class RuntimeController {

    private ConfigService configService;
    private ExecuteSessionService executeSessionService;
    private ConnectionPoolService connectionPoolService;
    private HMSMirrorAppService hmsMirrorAppService;

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

    @Autowired
    public void setHmsMirrorAppService(HMSMirrorAppService hmsMirrorAppService) {
        this.hmsMirrorAppService = hmsMirrorAppService;
    }


    public RunStatus start(String sessionId, Boolean dryrun) {
        // Check the current RunStatus.  If it is not STOPPED, then we cannot start.
        ExecuteSession session = executeSessionService.getSession(sessionId);
        RunStatus runStatus = session.getRunStatus();
        if (runStatus.getProgress() == ProgressEnum.IN_PROGRESS
                || runStatus.getProgress() == ProgressEnum.STARTED) {
            log.error("The session is currently running. Cannot start until operation has completed.");
            return false;
        } else {
            log.info("Starting session: " + sessionId);

        }
        runStatus.reset();
        runStatus.setProgress(ProgressEnum.STARTED);
        // Set the dryrun flag.
        executeSessionService.getSession(sessionId).getHmsMirrorConfig().setExecute(!dryrun);

        // Close all connections, so we can ensure we have a clean start.
        connectionPoolService.close();

        // Start job in a separate thread.
        Future<Boolean> runningTask = hmsMirrorAppService.run();

        // Set state to in progress.
        runStatus.setProgress(ProgressEnum.IN_PROGRESS);

        // Set the running task reference in the RunStatus.
        runStatus.setRunningTask(runningTask);

        return runStatus;
    }

    public RunStatus status(String sessionId) {
        return executeSessionService.getSession(sessionId).getRunStatus();
    }

    public RunStatus cancel(String sessionId) {
        // TODO: Stop a running session.

        return executeSessionService.getSession(sessionId).getRunStatus();
    }



}
