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
import com.cloudera.utils.hms.mirror.service.UIModelService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

@Controller
@RequestMapping(path = "/reports")
@Slf4j
public class ReportsMVController implements ControllerReferences {

    private UIModelService uiModelService;
    private ExecuteSessionService executeSessionService;

    @Autowired
    public void setUiModelService(UIModelService uiModelService) {
        this.uiModelService = uiModelService;
    }

    @Autowired
    public void setExecuteSessionService(ExecuteSessionService executeSessionService) {
        this.executeSessionService = executeSessionService;
    }

//    public ReportsMVController(ExecuteSessionService executeSessionService) {
//        this.executeSessionService = executeSessionService;
//    }

    @RequestMapping(value = "/list", method = RequestMethod.GET)
    public String listReports(Model model) {
        log.info("Listing reports");

        // Populate model
        uiModelService.sessionToModel(model, 1, Boolean.FALSE);
        // Get list of Reports
        model.addAttribute(REPORT_LIST, executeSessionService.getAvailableReports());

        return "reports/list";
    }


    @RequestMapping(value = "/view", method = RequestMethod.GET)
    public String viewReport(@RequestParam(value = REPORT_ID, required = true) String report_id) {
        log.info("Viewing report: {}", report_id);

        return "reports/view";
    }

    @RequestMapping(value = "/doDownload", method = RequestMethod.POST)
    public void doDownloadReport(@RequestParam(value = REPORT_ID, required = true) String report_id,
                HttpServletResponse response) {
        try {
            HttpEntity<ByteArrayResource> entity = executeSessionService.getZippedReport(report_id);
            response.setContentType("application/zip");
            // Translate headers
            entity.getHeaders().forEach((k, v) -> response.setHeader(k, v.get(0)));

            response.setHeader("Content-Disposition", "attachment; filename=\"" + report_id + ".zip\"");
            // get your file as InputStream
            InputStream is = Objects.requireNonNull(entity.getBody()).getInputStream();
            // copy it to response's OutputStream
            org.apache.commons.io.IOUtils.copy(is, response.getOutputStream());
            response.flushBuffer();
        } catch (IOException ex) {
            log.info("Error writing file to output stream. Filename was '{}.zip'", report_id, ex);
            throw new RuntimeException("IOError writing file to output stream");
        }

    }

}
