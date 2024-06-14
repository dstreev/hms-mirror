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

import com.cloudera.utils.hms.mirror.service.ExecuteSessionService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletResponse;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.Set;

@Controller
@RequestMapping(path = "/report")
@Slf4j
public class ReportMVController {

    private final ExecuteSessionService executeSessionService;

    public ReportMVController(ExecuteSessionService executeSessionService) {
        this.executeSessionService = executeSessionService;
    }

    @RequestMapping(value = "/view", method = RequestMethod.GET)
    public String viewReport(@RequestParam(value = "report_id", required = true) String report_id) {
        log.info("Viewing report: {}", report_id);

        return "/report/view";
    }

    @RequestMapping(value = "/download", method = RequestMethod.GET)
    public void downloadReport(@RequestParam(value = "report_id", required = true) String reportId,
                HttpServletResponse response) {
        try {
            HttpEntity<ByteArrayResource> entity = executeSessionService.getZippedReport(reportId);
            response.setContentType("application/zip");
            // Translate headers
            entity.getHeaders().forEach((k, v) -> response.setHeader(k, v.get(0)));

            response.setHeader("Content-Disposition", "attachment; filename=\"" + reportId + ".zip\"");
            // get your file as InputStream
            InputStream is = Objects.requireNonNull(entity.getBody()).getInputStream();
            // copy it to response's OutputStream
            org.apache.commons.io.IOUtils.copy(is, response.getOutputStream());
            response.flushBuffer();
        } catch (IOException ex) {
            log.info("Error writing file to output stream. Filename was '{}.zip'", reportId, ex);
            throw new RuntimeException("IOError writing file to output stream");
        }

    }

}