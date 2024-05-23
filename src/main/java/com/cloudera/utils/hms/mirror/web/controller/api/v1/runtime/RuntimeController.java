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
import com.cloudera.utils.hms.mirror.service.ExecuteSessionService;
import com.cloudera.utils.hms.mirror.web.service.RuntimeService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.validation.constraints.NotNull;
import java.io.File;
import java.io.FilenameFilter;
import java.util.Arrays;
import java.util.List;

@RestController
@Slf4j
@RequestMapping(path = "/api/v1/runtime")
public class RuntimeController {

    private ExecuteSessionService executeSessionService;
    private RuntimeService runtimeService;

    @Autowired
    public void setHmsMirrorCfgService(ExecuteSessionService executeSessionService) {
        this.executeSessionService = executeSessionService;
    }

    @Autowired
    public void setRuntimeService(RuntimeService runtimeService) {
        this.runtimeService = runtimeService;
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
    public RunStatus start(
//            @RequestParam(name = "sessionId", required = false) String sessionId,
                           @RequestParam(name = "dryrun") Boolean dryrun) {
        return runtimeService.start(dryrun);
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
    public RunStatus cancel() {
        RunStatus runStatus = executeSessionService.getActiveSession().getRunStatus();
        runStatus.cancel();
        return runStatus;
    }

    @Operation(summary = "Get Reports for Session")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Reports retrieved successfully",
                    content = {@Content(mediaType = "application/zip",
                            schema = @Schema(implementation = RunStatus.class))})
//            , @ApiResponse(responseCode = "400", description = "Invalid id supplied",
//                    content = @Content)
//            , @ApiResponse(responseCode = "404", description = "Config not found",
//                    content = @Content)
    })
    @ResponseBody
    @RequestMapping(method = RequestMethod.GET, value = "/reports/{id}")
    public void getSessionReports(@PathVariable @NotNull String id) {
        // Using the 'id', get the reports for the session.



        // Zip the files in the report directory and return the zip file.


    }

    @Operation(summary = "Available Reports")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "List of available reports",
                    content = {@Content(mediaType = "application/zip",
                            schema = @Schema(implementation = List.class))})
//            , @ApiResponse(responseCode = "400", description = "Invalid id supplied",
//                    content = @Content)
//            , @ApiResponse(responseCode = "404", description = "Config not found",
//                    content = @Content)
    })
    @ResponseBody
    @RequestMapping(method = RequestMethod.GET, value = "/reports/list")
    public List<String> availableReports() {
        List<String> rtn = null;
        // Validate that the report id directory exists.
        String reportDirectory = executeSessionService.getReportOutputDirectory();
        // List directories in the report directory.
        File folder = new File(reportDirectory);
        if (folder.isDirectory()) {
            String[] directories = folder.list(new FilenameFilter() {
                @Override
                public boolean accept(File current, String name) {
                    return new File(current, name).isDirectory();
                }
            });
            assert directories != null;
            rtn = Arrays.asList(directories);
        } else {
            // Throw exception that output directory isn't a directory.
        }
        return rtn;


    }

}
