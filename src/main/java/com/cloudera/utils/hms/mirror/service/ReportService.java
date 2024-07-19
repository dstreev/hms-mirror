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

import com.cloudera.utils.hms.mirror.DBMirror;
import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.support.RunStatus;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipOutputStream;

import static java.util.Objects.isNull;

@Component
@Slf4j
@Getter
@Setter
public class ReportService {

    private ExecuteSessionService executeSessionService;

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

    public List<String> databasesInReport(String id) {
        List<String> databases = new ArrayList<>();

        // Using the 'id', get the reports for the session.
        String reportDirectory = executeSessionService.getReportOutputDirectory();
        // List directories in the report directory.
        String sessionDirectoryName = reportDirectory + File.separator + id;
        File sessionDirectory = new File(sessionDirectoryName);

        List<String> files = Arrays.asList(sessionDirectory.list());

        for (String srcFile : files) {
            if (srcFile.endsWith("_hms-mirror.yaml")) {
//                String absolutePath = sessionDirectoryName + File.separator + srcFile;
                String databaseName = srcFile.substring(0, srcFile.indexOf("_hms-mirror.yaml"));

                databases.add(databaseName);
            }
        }
        return databases;
    }

    public String getDatabaseFile(String sessionId, String database) {
        String reportDirectory = executeSessionService.getReportOutputDirectory();
        return reportDirectory + File.separator + sessionId + File.separator + database + "_hms-mirror.yaml";
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
        HmsMirrorConfig config = HmsMirrorConfig.loadConfig(configFile);
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

        DBMirror dbMirror = null;
        log.info("Loading Report File: {}", databaseFile);
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        URL cfgUrl = null;
        try {
            // Load file from classpath and convert to string
            File cfgFile = new File(databaseFile);
            if (!cfgFile.exists()) {
                // Try loading from resource (classpath).  Mostly for testing.
                cfgUrl = mapper.getClass().getResource(databaseFile);
                if (isNull(cfgUrl)) {
                    throw new RuntimeException("Couldn't locate configuration file: " + databaseFile);
                }
                log.info("Using 'classpath' config: {}", databaseFile);
            } else {
                log.info("Using filesystem config: {}", databaseFile);
                try {
                    cfgUrl = cfgFile.toURI().toURL();
                } catch (MalformedURLException mfu) {
                    throw new RuntimeException("Couldn't locate configuration file: "
                            + databaseFile, mfu);
                }
            }

            String yamlCfgFile = IOUtils.toString(cfgUrl, StandardCharsets.UTF_8);
            dbMirror = mapper.readerFor(DBMirror.class).readValue(yamlCfgFile);


        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        log.info("Report loaded.");
        return dbMirror;

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
