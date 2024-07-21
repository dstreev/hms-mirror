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

import com.cloudera.utils.hms.mirror.domain.DBMirror;
import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.support.RunStatus;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipOutputStream;

import static com.cloudera.utils.hms.mirror.web.controller.ControllerReferences.DATABASES;
import static com.cloudera.utils.hms.mirror.web.controller.ControllerReferences.OTHERS;
import static java.util.Objects.isNull;

@Component
@Slf4j
@Getter
@Setter
public class ReportService {

    private DomainService domainService;
    private ExecuteSessionService executeSessionService;

    @Autowired
    public void setDomainService(DomainService domainService) {
        this.domainService = domainService;
    }

    @Autowired
    private void setExecuteSessionService(ExecuteSessionService executeSessionService) {
        this.executeSessionService = executeSessionService;
    }

    private void createZipFromDirectory(String zipFileName, String baseDirectory) throws IOException {

        final FileOutputStream fos = new FileOutputStream(zipFileName);
        ZipOutputStream zipOut = new ZipOutputStream(fos);
        File sessionDirectory = new File(baseDirectory);

        List<String> files = Arrays.asList(sessionDirectory.list());

        for (String srcFile : files) {
            System.out.println("Adding file: " + srcFile);
            String absolutePath = baseDirectory + File.separator + srcFile;
            File fileToZip = new File(absolutePath);
            if (fileToZip.exists()) {
                FileInputStream fis = null;
                try {
                    fis = new FileInputStream(fileToZip);
                    ZipEntry zipEntry = new ZipEntry(srcFile);
                    zipOut.putNextEntry(zipEntry);

                    byte[] bytes = new byte[1024];
                    int length;
                    while ((length = fis.read(bytes)) >= 0) {
                        zipOut.write(bytes, 0, length);
                    }
                } catch (ZipException ze) {
                    log.error(ze.getMessage(), ze);
                } finally {
                    assert fis != null;
                    fis.close();
                }
            } else {
                System.out.println("File not found: " + srcFile);
            }
        }
        zipOut.close();
        fos.close();
    }

    public Map<String, List<String>> reportArtifacts(String id) {
        Map<String, List<String>> artifacts = new TreeMap<>();

        // Using the 'id', get the reports for the session.
        String reportDirectory = executeSessionService.getReportOutputDirectory();
        // List directories in the report directory.
        String sessionDirectoryName = reportDirectory + File.separator + id;
        File sessionDirectory = new File(sessionDirectoryName);

        List<String> files = Arrays.asList(sessionDirectory.list());

        for (String srcFile : files) {
            if (srcFile.endsWith("_hms-mirror.yaml")) {
                String databaseName = srcFile.substring(0, srcFile.indexOf("_hms-mirror.yaml"));

                List<String> databases = artifacts.get(DATABASES);//.add(databaseName);
                if (isNull(databases)) {
                    databases = new ArrayList<>();
                    artifacts.put(DATABASES, databases);
                }
                databases.add(databaseName);
            } else {
                List<String> others = artifacts.get(OTHERS);
                if (isNull(others)) {
                    others = new ArrayList<>();
                    artifacts.put(OTHERS, others);
                }
                others.add(srcFile);
            }
        }
        return artifacts;
    }

    public String getDatabaseFile(String sessionId, String database) {
        String reportDirectory = executeSessionService.getReportOutputDirectory();
        return reportDirectory + File.separator + sessionId + File.separator + database + "_hms-mirror.yaml";
    }

    public String getReportFile(String sessionId, String reportFile) {
        String reportDirectory = executeSessionService.getReportOutputDirectory();
        return reportDirectory + File.separator + sessionId + File.separator + reportFile;
    }

    public String getSessionConfigFile(String sessionId) {
        String reportDirectory = executeSessionService.getReportOutputDirectory();
        return reportDirectory + File.separator + sessionId + File.separator + "session-config.yaml";
    }

    public String getSessionRunStatusFile(String sessionId) {
        String reportDirectory = executeSessionService.getReportOutputDirectory();
        return reportDirectory + File.separator + sessionId + File.separator + "run-status.yaml";
    }

    public HmsMirrorConfig getConfig(String sessionId) {
        String configFile = getSessionConfigFile(sessionId);
        log.info("Loading Config File: {}", configFile);
        HmsMirrorConfig config = domainService.deserializeConfig(configFile);
        return config;
    }

    public RunStatus getRunStatus(String sessionId) {
        String runStatusFile = getSessionRunStatusFile(sessionId);
        log.info("Loading RunStatus File: {}", runStatusFile);
        RunStatus status = RunStatus.loadConfig(runStatusFile);
        return status;
    }


    public DBMirror getDBMirror(String sessionId, String database) {
        String databaseFile = getDatabaseFile(sessionId, database);
        DBMirror dbMirror = domainService.deserializeDBMirror(databaseFile);
        log.info("Report loaded.");
        return dbMirror;
    }

    public String getReportFileString(String sessionId, String file) {
        String reportFile = getReportFile(sessionId, file);
        String asString = domainService.fileToString(reportFile);
        return asString;
    }

    public HttpEntity<ByteArrayResource> getZippedReport(String id) throws IOException {
        // Using the 'id', get the reports for the session.
        String reportDirectory = executeSessionService.getReportOutputDirectory();
        // List directories in the report directory.
        String sessionDirectoryName = reportDirectory + File.separator + id;
        File sessionDirectory = new File(sessionDirectoryName);
        // Ensure it exists and is a directory.
        if (!sessionDirectory.exists() || !sessionDirectory.isDirectory()) {
            throw new IOException("Session reports not found.");
        }

        // Zip the files in the report directory and return the zip file.
        String zipFileName = System.getProperty("java.io.tmpdir") + File.separator + id + ".zip";

        createZipFromDirectory(zipFileName, sessionDirectoryName);

        // Package and return the zip file.
        HttpHeaders header = new HttpHeaders();
        header.setContentType(new MediaType("application", "force-download"));

        String downloadFilename = id + ".zip";
        header.set(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + downloadFilename);

        return new HttpEntity<>(new ByteArrayResource(Files.readAllBytes(Paths.get(zipFileName))), header);
    }

    public Set<String> getAvailableReports() {
        Set<String> rtn = new TreeSet<>(new Comparator<String>() {
            // Descending order.
            @Override
            public int compare(String o1, String o2) {
                return o2.compareTo(o1);
            }
        });
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
//            List<String> dirList = Arrays.asList(directories);
            rtn.addAll(Arrays.asList(directories));
//            rtn = Arrays.asList(directories);
        } else {
            // Throw exception that output directory isn't a directory.
        }
        return rtn;
    }


}
