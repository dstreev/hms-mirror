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

package com.cloudera.utils.hms.mirror.web.service;

import com.cloudera.utils.hms.mirror.domain.support.ExecuteSession;
import com.cloudera.utils.hms.mirror.domain.support.ProgressEnum;
import com.cloudera.utils.hms.mirror.domain.support.RunStatus;
import com.cloudera.utils.hms.mirror.service.ExecuteSessionService;
import com.cloudera.utils.hms.mirror.service.HMSMirrorAppService;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.concurrent.Future;

@Service
@Getter
@Setter
@Slf4j
public class RuntimeService {

    private ExecuteSessionService executeSessionService;
    private HMSMirrorAppService hmsMirrorAppService;

    @Autowired
    public void setExecuteSessionService(ExecuteSessionService executeSessionService) {
        this.executeSessionService = executeSessionService;
    }

    @Autowired
    public void setHmsMirrorAppService(HMSMirrorAppService hmsMirrorAppService) {
        this.hmsMirrorAppService = hmsMirrorAppService;
    }

    public RunStatus start(Boolean dryrun) {
        ExecuteSession session = executeSessionService.transitionLoadedSessionToActive();

        RunStatus runStatus = session.getRunStatus();
        if (runStatus.getProgress() == ProgressEnum.IN_PROGRESS
                || runStatus.getProgress() == ProgressEnum.STARTED
                || runStatus.getProgress() == ProgressEnum.CANCEL_FAILED) {
            log.error("The session is currently running. Cannot start until operation has completed.");
            throw new RuntimeException("Session already running.");
        }

        if (runStatus.reset()) {
            runStatus.setProgress(ProgressEnum.STARTED);
            // Set the dryrun flag.
            executeSessionService.getActiveSession().getResolvedConfig().setExecute(!dryrun);

            // Start job in a separate thread.
            Future<Boolean> runningTask = hmsMirrorAppService.run();

            // Set state to in progress.
            runStatus.setProgress(ProgressEnum.IN_PROGRESS);

            // Set the running task reference in the RunStatus.
            runStatus.setRunningTask(runningTask);
        }
        return runStatus;

    }
}
