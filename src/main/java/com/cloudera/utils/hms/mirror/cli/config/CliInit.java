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

package com.cloudera.utils.hms.mirror.cli.config;

import com.cloudera.utils.hms.mirror.DBMirror;
import com.cloudera.utils.hms.mirror.Environment;
import com.cloudera.utils.hms.mirror.domain.Cluster;
import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.TableMirror;
import com.cloudera.utils.hms.mirror.domain.support.Conversion;
import com.cloudera.utils.hms.mirror.domain.support.RunStatus;
import com.cloudera.utils.hms.mirror.service.ExecuteSessionService;
import com.cloudera.utils.hms.util.TableUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;

@Configuration
@Slf4j
public class CliInit {

    private ExecuteSessionService executeSessionService;

    @Autowired
    public void setExecuteSessionService(ExecuteSessionService executeSessionService) {
        this.executeSessionService = executeSessionService;
    }

    private HmsMirrorConfig initializeConfig(String configFilename) {
        HmsMirrorConfig hmsMirrorConfig;
        log.info("Initializing Config.");
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        URL cfgUrl = null;
        try {
            // Load file from classpath and convert to string
            File cfgFile = new File(configFilename);
            if (!cfgFile.exists()) {
                // Try loading from resource (classpath).  Mostly for testing.
                cfgUrl = this.getClass().getResource(configFilename);
                if (cfgUrl == null) {
                    throw new RuntimeException("Couldn't locate configuration file: " + configFilename);
                }
                log.info("Using 'classpath' config: {}", configFilename);
            } else {
                log.info("Using filesystem config: {}", configFilename);
                try {
                    cfgUrl = cfgFile.toURI().toURL();
                } catch (MalformedURLException mfu) {
                    throw new RuntimeException("Couldn't locate configuration file: "
                            + configFilename, mfu);
                }
            }

            String yamlCfgFile = IOUtils.toString(cfgUrl, StandardCharsets.UTF_8);
            hmsMirrorConfig = mapper.readerFor(HmsMirrorConfig.class).readValue(yamlCfgFile);
//            hmsMirrorConfig.setRunStatus(runStatus);
            // Link the translator to the config
            hmsMirrorConfig.getTranslator().setHmsMirrorConfig(hmsMirrorConfig);
            for (Cluster cluster : hmsMirrorConfig.getClusters().values()) {
                cluster.setHmsMirrorConfig(hmsMirrorConfig);
            }
            hmsMirrorConfig.setConfigFilename(configFilename);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        log.info("Config loaded.");
        log.info("Transfer Concurrency: {}", hmsMirrorConfig.getTransfer().getConcurrency());
        return hmsMirrorConfig;
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.setup",
            havingValue = "false")
    public HmsMirrorConfig loadHmsMirrorConfig(@Value("${hms-mirror.config.path}") String configPath,
                                               @Value("${hms-mirror.config.file}") String configFile) {
        String fullConfigPath;
        // If file is absolute, use it.  Otherwise, use the path.
        if (configFile.startsWith(File.separator)) {
            fullConfigPath = configFile;
        } else {
            fullConfigPath = configPath + File.separator + configFile;
        }
        return HmsMirrorConfig.loadConfig(fullConfigPath);
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.setup",
            havingValue = "true")
    /*
    Init empty for framework to fill in.
     */
    public HmsMirrorConfig loadHmsMirrorConfigWithSetup() {
        return new HmsMirrorConfig();
    }

//    @Bean(name = "runStatus")
//    @Order(10)
//    @ConditionalOnProperty(
//            name = "hms-mirror.conversion.test-filename",
//            havingValue = "false")
//    public RunStatus buildRunStatus() {
//        log.info("Building Clean RunStatus Instance");
//        return new RunStatus();
//    }

    @Bean
    @Order(500)
    @ConditionalOnProperty(
            name = "hms-mirror.conversion.test-filename")
    public CommandLineRunner loadTestData(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.conversion.test-filename}") String filename) throws IOException {
//        RunStatus runStatus = new RunStatus();
        return args -> {

            hmsMirrorConfig.setLoadTestDataFile(filename);
            log.info("Reconstituting Conversion from test data file: {}", filename);
            try {
                log.info("Check 'classpath' for test data file");
                URL configURL = this.getClass().getResource(filename);
                if (configURL == null) {
                    log.info("Checking filesystem for test data file");
                    File conversionFile = new File(filename);
                    if (!conversionFile.exists())
                        throw new RuntimeException("Couldn't locate test data file: " + filename);
                    configURL = conversionFile.toURI().toURL();
                }
                ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
                mapper.enable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

                String yamlCfgFile = IOUtils.toString(configURL, StandardCharsets.UTF_8);
                Conversion conversion = mapper.readerFor(Conversion.class).readValue(yamlCfgFile);
                // Set Config Databases;
                hmsMirrorConfig.setDatabases(conversion.getDatabases().keySet().toArray(new String[0]));
                executeSessionService.getCurrentSession().setConversion(conversion);
//            runStatus.setConversion(conversion);
            } catch (UnrecognizedPropertyException upe) {
                throw new RuntimeException("\nThere may have been a breaking change in the configuration since the previous " +
                        "release. Review the note below and remove the 'Unrecognized field' from the configuration and try " +
                        "again.\n\n", upe);
            } catch (Throwable t) {
                // Look for yaml update errors.
                if (t.toString().contains("MismatchedInputException")) {
                    throw new RuntimeException("The format of the 'config' yaml file MAY HAVE CHANGED from the last release.  Please make a copy and run " +
                            "'-su|--setup' again to recreate in the new format", t);
                } else {
                    log.error(t.getMessage(), t);
                    throw new RuntimeException("A configuration element is no longer valid, progress.  Please remove the element from the configuration yaml and try again.", t);
                }
            }
        };
//        return runStatus;
    }

    @Bean
    // Needs to happen after all the configs have been set.
    @Order(15)
    public CommandLineRunner conversionPostProcessing(HmsMirrorConfig hmsMirrorConfig) {
        return args -> {
            executeSessionService.getCurrentSession().setHmsMirrorConfig(hmsMirrorConfig);
            RunStatus runStatus = executeSessionService.getCurrentSession().getRunStatus();
            Conversion conversion = executeSessionService.getCurrentSession().getConversion();

            log.info("Post Processing Conversion");
            if (hmsMirrorConfig.isLoadingTestData()) {
                for (DBMirror dbMirror : conversion.getDatabases().values()) {
                    String database = dbMirror.getName();
                    for (TableMirror tableMirror : dbMirror.getTableMirrors().values()) {
                        String tableName = tableMirror.getName();
                        if (TableUtils.isACID(tableMirror.getEnvironmentTable(Environment.LEFT))
                                && !hmsMirrorConfig.getMigrateACID().isOn()) {
                            tableMirror.setRemove(true);
                        } else if (!TableUtils.isACID(tableMirror.getEnvironmentTable(Environment.LEFT))
                                && hmsMirrorConfig.getMigrateACID().isOnly()) {
                            tableMirror.setRemove(true);
                        } else {
                            // Same logic as in TableService.getTables to filter out tables that are not to be processed.
                            if (tableName.startsWith(hmsMirrorConfig.getTransfer().getTransferPrefix())) {
                                log.info("Database: {} Table: {} was NOT added to list.  The name matches the transfer prefix and is most likely a remnant of a previous event. If this is a mistake, change the 'transferPrefix' to something more unique.", database, tableName);
                                tableMirror.setRemove(true);
                            } else if (tableName.endsWith("storage_migration")) {
                                log.info("Database: {} Table: {} was NOT added to list.  The name is the result of a previous STORAGE_MIGRATION attempt that has not been cleaned up.", database, tableName);
                                tableMirror.setRemove(true);
                            } else {
                                if (hmsMirrorConfig.getFilter().getTblRegEx() != null) {
                                    // Filter Tables
                                    assert (hmsMirrorConfig.getFilter().getTblFilterPattern() != null);
                                    Matcher matcher = hmsMirrorConfig.getFilter().getTblFilterPattern().matcher(tableName);
                                    if (!matcher.matches()) {
                                        log.info("{}:{} didn't match table regex filter and will NOT be added to processing list.", database, tableName);
                                        tableMirror.setRemove(true);
                                    }
                                } else if (hmsMirrorConfig.getFilter().getTblExcludeRegEx() != null) {
                                    assert (hmsMirrorConfig.getFilter().getTblExcludeFilterPattern() != null);
                                    Matcher matcher = hmsMirrorConfig.getFilter().getTblExcludeFilterPattern().matcher(tableName);
                                    if (matcher.matches()) { // ANTI-MATCH
                                        log.info("{}:{} matched exclude table regex filter and will NOT be added to processing list.", database, tableName);
                                        tableMirror.setRemove(true);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            // Remove Tables from Map.
            for (DBMirror dbMirror : conversion.getDatabases().values()) {
                dbMirror.getTableMirrors().entrySet().removeIf(entry -> entry.getValue().isRemove());
            }
        };
    }

}
