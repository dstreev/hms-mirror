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

import com.cloudera.utils.hms.mirror.DBMirror;
import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.support.RunStatus;
import com.cloudera.utils.hms.mirror.service.ExecuteSessionService;
import com.cloudera.utils.hms.mirror.service.ReportService;
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
import java.util.List;
import java.util.Objects;

@Controller
@RequestMapping(path = "/reports")
@Slf4j
public class ReportsMVController implements ControllerReferences {

    private ReportService reportService;
    private UIModelService uiModelService;
    private ExecuteSessionService executeSessionService;

    @Autowired
    public void setUiModelService(UIModelService uiModelService) {
        this.uiModelService = uiModelService;
    }

    @Autowired
    public void setReportService(ReportService reportService) {
        this.reportService = reportService;
    }

    @Autowired
    public void setExecuteSessionService(ExecuteSessionService executeSessionService) {
        this.executeSessionService = executeSessionService;
    }

//    public ReportsMVController(ExecuteSessionService executeSessionService) {
//        this.executeSessionService = executeSessionService;
//    }

    @RequestMapping(value = "/select", method = RequestMethod.GET)
    public String listReports(Model model) {
        log.info("Listing reports");

        // Populate model
        uiModelService.sessionToModel(model, 1, Boolean.FALSE);
        // Get list of Reports
        model.addAttribute(REPORT_LIST, reportService.getAvailableReports());
        model.addAttribute(ACTION, "select"); // Supports which form fragment is loaded.
        return "reports/view";
    }

    @RequestMapping(value = "/dbdetail", method = RequestMethod.GET)
    public String viewReport(Model model,
                             @RequestParam(value = REPORT_ID, required = true) String report_id,
                             @RequestParam(value = DATABASE, required = true) String database) {
        DBMirror dbMirror = reportService.getDBMirror(report_id, database);
        model.addAttribute(DB_MIRROR, dbMirror);
        HmsMirrorConfig config = reportService.getConfig(report_id);
        model.addAttribute(CONFIG, config);
        RunStatus runStatus = reportService.getRunStatus(report_id);
        model.addAttribute(RUN_STATUS, runStatus);
        return "reports/dbdetail";
    }


    @RequestMapping(value = "/detail", method = RequestMethod.GET)
    public String viewReport(Model model,
                             @RequestParam(value = REPORT_ID, required = true) String report_id) {
        log.info("Viewing report: {}", report_id);
        // Populate model
        uiModelService.sessionToModel(model, 1, Boolean.FALSE);
        // Get list of Reports
        List<String> databases = reportService.databasesInReport(report_id);
        model.addAttribute(DATABASES, databases);
        model.addAttribute(SESSION_ID, report_id);
//        model.addAttribute(REPORT_LIST, reportService.getAvailableReports());

        model.addAttribute(ACTION, "detail"); // Supports which form fragment is loaded.
        return "reports/view";
    }

    @RequestMapping(value = "/doDownload", method = RequestMethod.POST)
    public void doDownloadReport(@RequestParam(value = REPORT_ID, required = true) String report_id,
                HttpServletResponse response) {
        try {
            HttpEntity<ByteArrayResource> entity = reportService.getZippedReport(report_id);
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
