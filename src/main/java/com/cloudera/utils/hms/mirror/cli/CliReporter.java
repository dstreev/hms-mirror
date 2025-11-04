/*
 * Copyright (c) 2023-2025. Cloudera, Inc. All Rights Reserved
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

package com.cloudera.utils.hms.mirror.cli;

import com.cloudera.utils.hms.mirror.domain.core.DBMirror;
import com.cloudera.utils.hms.mirror.domain.core.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.core.TableMirror;
import com.cloudera.utils.hms.mirror.domain.dto.ConfigLiteDto;
import com.cloudera.utils.hms.mirror.domain.dto.JobDto;
import com.cloudera.utils.hms.mirror.domain.support.*;
import com.cloudera.utils.hms.mirror.exceptions.RepositoryException;
import com.cloudera.utils.hms.mirror.reporting.ReportingConf;
import com.cloudera.utils.hms.mirror.repository.DBMirrorRepository;
import com.cloudera.utils.hms.mirror.repository.TableMirrorRepository;
import com.cloudera.utils.hms.mirror.service.ConversionResultService;
import com.cloudera.utils.hms.mirror.service.ExecutionContextService;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.core.annotation.Order;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.FileSystems;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.commons.lang3.StringUtils.isBlank;

@Component
@Getter
@Setter
@Slf4j
@RequiredArgsConstructor
public class CliReporter {
    private final Date start = new Date();
    private final int sleepInterval = 1000;
    private final List<String> reportTemplateHeader = new ArrayList<>();
    private final List<String> reportTemplateTableDetail = new ArrayList<>();
    private final List<String> reportTemplateFooter = new ArrayList<>();
    private final List<String> reportTemplateOutput = new ArrayList<>();
    private final Map<String, String> varMap = new TreeMap<>();
    private final List<TableMirror> startedTables = new ArrayList<>();

    @NonNull
    private final ExecutionContextService executionContextService;
    @NonNull
    private final ConversionResultService conversionResultService;
    @NonNull
    private final DBMirrorRepository dbMirrorRepository;
    @NonNull
    private final TableMirrorRepository tableMirrorRepository;

    private Thread worker;
    private Boolean retry = Boolean.FALSE;
    private Boolean quiet = Boolean.FALSE;
    private boolean tiktok = false;

    @Bean
    @Order(20)
    CommandLineRunner configQuiet(HmsMirrorConfig hmsMirrorConfig) {
        return args -> setQuiet(hmsMirrorConfig.isQuiet());
    }

    protected void displayReport(Boolean showAll) {
        ConversionResult conversionResult = getExecutionContextService().getConversionResult().orElseThrow(() ->
                new IllegalStateException("ConversionResult not set."));

        System.out.print(ReportingConf.CLEAR_CONSOLE);
        StringBuilder report = new StringBuilder();
        // Header
        if (!quiet) {
            report.append(ReportingConf.substituteAllVariables(reportTemplateHeader, varMap));

            // Table Processing
            for (TableMirror tblMirror : startedTables) {
                Map<String, String> tblVars = new TreeMap<>();
                String dbName = findDatabaseForTable(conversionResult, tblMirror);
                tblVars.put("db.name", getConversionResultService().getResolvedDB(dbName));
                tblVars.put("tbl.name", tblMirror.getName());
                tblVars.put("tbl.progress", tblMirror.getProgressIndicator(80));
                tblVars.put("tbl.msg", tblMirror.getMigrationStageMessage());
                tblVars.put("tbl.strategy", tblMirror.getStrategy().toString());
                report.append(ReportingConf.substituteAllVariables(reportTemplateTableDetail, tblVars));
            }
        }

        // Footer
        report.append(ReportingConf.substituteAllVariables(reportTemplateFooter, varMap));
        List<String> databases;
        try {
            databases = getDbMirrorRepository().listNamesByKey(conversionResult.getKey());
        } catch (RepositoryException e) {
            throw new RuntimeException(e);
        }
        // Output
        if (showAll) {
            report.append("\nDatabases(<db>):\n");
            report.append(String.join(",", databases));
            report.append("\n");
            report.append(ReportingConf.substituteAllVariables(reportTemplateOutput, varMap));
            log.info(report.toString());

            report.append(getMessages());

        }

        System.out.print(report);

    }

    public String getMessages() {
        StringBuilder report = new StringBuilder();
        ConversionResult conversionResult = getExecutionContextService().getConversionResult().orElseThrow(() ->
                new IllegalStateException("ConversionResult not set."));
        ConfigLiteDto config = conversionResult.getConfig();
        RunStatus runStatus = conversionResult.getRunStatus();

        if (runStatus.hasErrors()) {
            report.append("\n=== Errors ===\n");
            for (String message : runStatus.getErrorMessages()) {
                report.append("\t").append(message).append("\n");
            }
        }

        if (runStatus.hasWarnings()) {
            report.append("\n=== Warnings ===\n");
            for (String message : runStatus.getWarningMessages()) {
                report.append("\t").append(message).append("\n");
            }
        }

        return report.toString();
    }

    private void fetchReportTemplates() throws IOException {

        InputStream his = this.getClass().getResourceAsStream(!quiet ? "/report_header.txt" : "/quiet/report_header.txt");
        BufferedReader hbr = new BufferedReader(new InputStreamReader(his));
        String hline = null;
        while ((hline = hbr.readLine()) != null) {
            reportTemplateHeader.add(hline);
        }
        InputStream fis = this.getClass().getResourceAsStream(!quiet ? "/report_footer.txt" : "/quiet/report_footer.txt");
        BufferedReader fbr = new BufferedReader(new InputStreamReader(fis));
        String fline = null;
        while ((fline = fbr.readLine()) != null) {
            reportTemplateFooter.add(fline);
        }
        InputStream fisop = this.getClass().getResourceAsStream(!quiet ? "/report_output.txt" : "/quiet/report_output.txt");
        BufferedReader fbrop = new BufferedReader(new InputStreamReader(fisop));
        String flineop = null;
        while ((flineop = fbrop.readLine()) != null) {
            reportTemplateOutput.add(flineop);
        }
        InputStream tis = this.getClass().getResourceAsStream(!quiet ? "/table_display.txt" : "/quiet/table_display.txt");
        BufferedReader tbr = new BufferedReader(new InputStreamReader(tis));
        String tline = null;
        while ((tline = tbr.readLine()) != null) {
            reportTemplateTableDetail.add(tline);
        }

    }

    /*
    Go through the Conversion object and set the variables.
     */
    private void populateVarMap() {
        ConversionResult conversionResult = getExecutionContextService().getConversionResult().orElseThrow(() ->
                new IllegalStateException("ConversionResult not set."));
        HmsMirrorConfig config = getExecutionContextService().getHmsMirrorConfig().orElseThrow(() ->
                new IllegalStateException("HmsMirrorConfig not set"));

//        ConfigLiteDto config = conversionResult.getConfig();
        JobDto job = conversionResult.getJob();
        JobExecution jobExecution = conversionResult.getJobExecution();

        tiktok = !tiktok;
        startedTables.clear();
        if (!retry)
            varMap.put("retry", "       ");
        else
            varMap.put("retry", "(RETRY)");
        varMap.put("run.mode", jobExecution.isExecute() ? "EXECUTE" : "DRYRUN");
        varMap.put("HMS-Mirror-Version", ReportingConf.substituteVariablesFromManifest("${HMS-Mirror-Version}"));
        // TODO: Fix.  We may need to rearrange this to work for the Web UI too.
        varMap.put("config.file", config.getConfigFilename());
        varMap.put("config.strategy", job.getStrategy().toString());
        varMap.put("tik.tok", tiktok ? "*" : "");
        varMap.put("java.version", System.getProperty("java.version"));
        varMap.put("os.name", System.getProperty("os.name"));
        varMap.put("cores", Integer.toString(Runtime.getRuntime().availableProcessors()));
        varMap.put("os.arch", System.getProperty("os.arch"));
        varMap.put("memory", Runtime.getRuntime().totalMemory() / 1024 / 1024 + "MB");

        String outputDir = config.getOutputDirectory();
        if (!isBlank(config.getFinalOutputDirectory())) {
            outputDir = config.getFinalOutputDirectory();
        }

        varMap.put("report.file", outputDir + FileSystems.getDefault().getSeparator() + "<db>_hms-mirror.md|html|yaml");
        varMap.put("left.execute.file", outputDir + FileSystems.getDefault().getSeparator() + "<db>_LEFT_execute.sql");

        varMap.put("left.cleanup.file", outputDir + FileSystems.getDefault().getSeparator() + "<db>_LEFT_CleanUp_execute.sql");
        varMap.put("right.execute.file", outputDir + FileSystems.getDefault().getSeparator() + "<db>_RIGHT_execute.sql");
        varMap.put("right.cleanup.file", outputDir + FileSystems.getDefault().getSeparator() + "<db>_RIGHT_CleanUp_execute.sql");

        varMap.put("total.dbs", Integer.toString(conversionResult.getDatabases().size()));
        // Count
        final AtomicInteger tblCount = new AtomicInteger(0);

//        List<String> dbNames = getDbMirrorRepository().listNamesByKey(conversionResult.getKey());
//        dbNames.forEach(dbName -> {
//            List<String> tableNames = getTableMirrorRepository().listNamesByKey(conversionResult.getKey(), dbName);
//            tblCount += tableNames.size();
//        });
//

        // Table Counters
        final AtomicInteger started = new AtomicInteger(0);
        final AtomicInteger completed = new AtomicInteger(0);
        final AtomicInteger errors = new AtomicInteger(0);
        final AtomicInteger skipped = new AtomicInteger(0);

        List<String> dbNames = null;
        try {
            dbNames = getDbMirrorRepository().listNamesByKey(conversionResult.getKey());
        } catch (RepositoryException e) {
            throw new RuntimeException(e);
        }
        dbNames.forEach(dbName -> {
            Map<String, TableMirror> tables = null;
            try {
                tables = getTableMirrorRepository().findByDatabase(conversionResult.getKey(), dbName);
            } catch (RepositoryException e) {
                throw new RuntimeException(e);
            }
            tblCount.addAndGet(tables.size());
            tables.forEach((tblName, tableMirror) -> {
                switch (tableMirror.getPhaseState()) {
                    case INIT:
                        break;
                    case APPLYING_SQL:
                    case CALCULATING_SQL:
                        started.incrementAndGet();
                        startedTables.add(tableMirror);
                        break;
                    case CALCULATED_SQL:
                        if (config.isExecute())
                            started.incrementAndGet();
                        else
                            completed.incrementAndGet();
                        break;
                    case PROCESSED:
                        completed.incrementAndGet();
                        break;
                    case ERROR:
                    case CALCULATED_SQL_WARNING:
                        errors.incrementAndGet();
                        break;
                    case RETRY_SKIPPED_PAST_SUCCESS:
                        skipped.incrementAndGet();
                }
            });

        });

        varMap.put("total.tbls", Integer.toString(tblCount.get()));

        varMap.put("started.tbls", Integer.toString(started.get()));
        varMap.put("completed.tbls", Integer.toString(completed.get()));
        varMap.put("error.tbls", Integer.toString(errors.get()));
        varMap.put("skipped.tbls", Integer.toString(skipped.get()));
        Date current = new Date();
        long elapsedMS = current.getTime() - start.getTime();
        if (tiktok)
            varMap.put("elapsed.time", "\u001B[34m" + elapsedMS / 1000 + "[0m");
        else
            varMap.put("elapsed.time", "\u001B[33m" + elapsedMS / 1000 + "[0m");
    }

    public void refresh(Boolean showAll) {
        try {
            populateVarMap();
            displayReport(showAll);
        } catch (ConcurrentModificationException cme) {
            log.error("Report Refresh", cme);
        }
    }

    @Async("reportingThreadPool")
    public CompletableFuture<Void> run(ConversionResult conversionResult, HmsMirrorConfig config) {
        getExecutionContextService().setConversionResult(conversionResult);
        getExecutionContextService().setRunStatus(conversionResult.getRunStatus());
        getExecutionContextService().setHmsMirrorConfig(config);

        RunStatus runStatus = conversionResult.getRunStatus();
        CompletableFuture<Void> future;

        future = CompletableFuture.runAsync(() -> {
            getExecutionContextService().setConversionResult(conversionResult);
            getExecutionContextService().setRunStatus(conversionResult.getRunStatus());
            getExecutionContextService().setHmsMirrorConfig(config);
            try {
                fetchReportTemplates();
                log.info("Starting Reporting Thread");
                // Wait for the main thread to start.
                while (runStatus.getProgress().equals(ProgressEnum.INITIALIZED)) {
                    try {
                        Thread.sleep(sleepInterval);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                // If the Job hasn't completed, keep cycling.
                while (EnumSet.of(ProgressEnum.STARTED, ProgressEnum.IN_PROGRESS).contains(runStatus.getProgress())) {
                    refresh(Boolean.FALSE);
                    try {
                        Thread.sleep(sleepInterval);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                log.info("Completed Reporting Thread");
            } catch (IOException ioe) {
                System.out.println("Missing Reporting Template");
            }
        });
        return future;
    }

    @PreDestroy
    public void cleanup() {
        log.info("Cleaning up Reporting Thread");
    }

    public void setVariable(String key, String value) {
        varMap.put(key, value);
    }

    private String findDatabaseForTable(ConversionResult conversionResult, TableMirror tableMirror) {
        for (Map.Entry<String, DBMirror> entry : conversionResult.getDatabases().entrySet()) {
            DBMirror dbMirror = entry.getValue();
            if (dbMirror.getTableMirrors() != null && dbMirror.getTableMirrors().containsValue(tableMirror)) {
                return entry.getKey();
            }
        }
        return null; // Table not found in any database
    }

}