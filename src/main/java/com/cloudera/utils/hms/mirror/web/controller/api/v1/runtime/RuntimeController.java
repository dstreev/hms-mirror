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
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

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

    @Operation(summary = "Start the Operation")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Operation started successfully",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = RunStatus.class))})
//            , @ApiResponse(responseCode = "400", description = "Invalid id supplied",
//                    content = @Content)
//            , @ApiResponse(responseCode = "404", description = "Config not found",
//                    content = @Content)
    })
    @ResponseBody
    @RequestMapping(method = RequestMethod.POST, value = "/start")
    public RunStatus start(@RequestParam(name = "sessionId", required = false) String sessionId,
                           @RequestParam(name = "dryrun") Boolean dryrun) {
        // Check the current RunStatus.  If it is not STOPPED, then we cannot start.
        ExecuteSession session = executeSessionService.getCurrentSession();
        RunStatus runStatus = session.getRunStatus();
        if (runStatus.getProgress() == ProgressEnum.IN_PROGRESS
                || runStatus.getProgress() == ProgressEnum.STARTED
                || runStatus.getProgress() == ProgressEnum.CANCEL_FAILED) {
            log.error("The session is currently running. Cannot start until operation has completed.");
            throw new RuntimeException("Session already running.");
        } else {
            log.info("Starting session: " + sessionId);
        }

        if (runStatus.reset()) {
            runStatus.setProgress(ProgressEnum.STARTED);
            // Set the dryrun flag.
            executeSessionService.getCurrentSession().getHmsMirrorConfig().setExecute(!dryrun);

            // Moved to HmsMirrorAppService.run()
//            // Establish the connection pools.
//            connectionPoolService.close();
//            try {
//                connectionPoolService.init();
//            } catch (Exception e) {
//                log.error("Error initializing connection pools.", e);
//                runStatus.setProgress(ProgressEnum.FAILED);
//                return runStatus;
//            }

            // Start job in a separate thread.
            Future<Boolean> runningTask = hmsMirrorAppService.run();

            // Set state to in progress.
            runStatus.setProgress(ProgressEnum.IN_PROGRESS);

            // Set the running task reference in the RunStatus.
            runStatus.setRunningTask(runningTask);
        }
        return runStatus;
    }


    @Operation(summary = "Cancel the Operation")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Operation cancelled successfully",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = RunStatus.class))})
//            , @ApiResponse(responseCode = "400", description = "Invalid id supplied",
//                    content = @Content)
//            , @ApiResponse(responseCode = "404", description = "Config not found",
//                    content = @Content)
    })
    @ResponseBody
    @RequestMapping(method = RequestMethod.POST, value = "/cancel")
    public RunStatus cancel(String sessionId) {
        RunStatus runStatus = executeSessionService.getCurrentSession().getRunStatus();
        runStatus.cancel();
        return runStatus;
    }


}
