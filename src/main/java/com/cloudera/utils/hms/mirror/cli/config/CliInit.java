/*
 * Copyright (c) 2024-2025. Cloudera, Inc. All Rights Reserved
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

import com.cloudera.utils.hms.mirror.domain.core.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.support.ConversionResult;
import com.cloudera.utils.hms.mirror.domain.support.RunStatus;
import com.cloudera.utils.hms.mirror.domain.testdata.LegacyConversionWrapper;
import com.cloudera.utils.hms.mirror.service.ConversionResultService;
import com.cloudera.utils.hms.mirror.service.ExecutionContextService;
import com.cloudera.utils.hms.mirror.util.HmsMirrorConfigConverter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import static java.util.Objects.isNull;

@Component
@Slf4j
@Getter
//@RequiredArgsConstructor
public class CliInit {

    @NonNull
    private final ConversionResultService conversionResultService;
    @NonNull
    private final ExecutionContextService executionContextService;
    @NonNull
    private final ObjectMapper yamlMapper;
    @NonNull

    public CliInit(@NonNull ConversionResultService conversionResultService,
                   @NonNull ExecutionContextService executionContextService,
                   @NonNull @Qualifier("yamlMapper") ObjectMapper yamlMapper
    ) {
        this.conversionResultService = conversionResultService;
        this.executionContextService = executionContextService;
        this.yamlMapper = yamlMapper;
    }

    /**
     * Initializes the HmsMirrorConfig object by loading the configuration information
     * from the specified YAML file. This method attempts to locate the configuration file
     * first from the filesystem and then from the classpath, providing compatibility for
     * various runtime environments.
     *
     * @param configFilename the name of the configuration file to load; it can be a file path
     *                       or a resource name on the classpath.
     * @return the initialized HmsMirrorConfig object created from the provided configuration file.
     * @throws RuntimeException if the configuration file cannot be found or if an I/O error
     *                          occurs while reading or parsing the file.
     */
    private HmsMirrorConfig initializeConfig(String configFilename) {
        HmsMirrorConfig config;
        log.info("Initializing Config.");
//        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        yamlMapper.enable(SerializationFeature.INDENT_OUTPUT);
        URL cfgUrl = null;
        try {
            // Load file from classpath and convert to string
            File cfgFile = new File(configFilename);
            if (!cfgFile.exists()) {
                // Try loading from resource (classpath).  Mostly for testing.
                cfgUrl = this.getClass().getResource(configFilename);
                if (isNull(cfgUrl)) {
                    log.error("Couldn't locate configuration file: {}", configFilename);
                    throw new RuntimeException("Couldn't locate configuration file: " + configFilename);
                }
                log.info("Using 'classpath' config: {}", configFilename);
            } else {
                log.info("Using filesystem config: {}", configFilename);
                try {
                    cfgUrl = cfgFile.toURI().toURL();
                } catch (MalformedURLException mfu) {
                    log.error("Couldn't location configuration file: {}", configFilename);
                    throw new RuntimeException("Couldn't locate configuration file: "
                            + configFilename, mfu);
                }
            }

            String yamlCfgFile = IOUtils.toString(cfgUrl, StandardCharsets.UTF_8);
            config = yamlMapper.readerFor(HmsMirrorConfig.class).readValue(yamlCfgFile);
            config.setConfigFilename(configFilename);
        } catch (IOException e) {
            log.error("IO Exception", e);
            throw new RuntimeException(e);
        }
        log.info("Config loaded.");
        return config;
    }

//    private void loadTestData() {
//        log.info("Loading Test Data");
//        HmsMirrorConfig config = getExecutionContextService().getHmsMirrorConfig().orElseThrow(() ->
//                new IllegalStateException("HmsMirrorConfig not set"));
//        ConversionResult conversionResult = getExecutionContextService().getConversionResult().orElseThrow(() ->
//                new IllegalStateException("ConversionResult not set"));
//        conversionResult.setMockTestDataset(Boolean.TRUE);
//        String yamlCfgFile = null;
//        String filename = config.getLoadTestDataFile();
//        try {
//            DBMirrorTest dbMirrorTest = loadDBMirrorFromFile(config.getLoadTestDataFile());
//            getConversionResultService().loadDBMirrorTest(dbMirrorTest);
//
//            //            log.info("Reconstituting Conversion from test data file: {}", filename);
////            log.info("Checking 'classpath' for test data file");
////            URL configURL = this.getClass().getResource(filename);
////            if (isNull(configURL)) {
////                log.info("Checking filesystem for test data file: {}", filename);
////                File conversionFile = new File(filename);
////                if (!conversionFile.exists()) {
////                    log.error("Couldn't locate test data file: {}", filename);
////                    throw new RuntimeException("Couldn't locate test data file: " + filename);
////                }
////                configURL = conversionFile.toURI().toURL();
////            }
////
////            yamlCfgFile = IOUtils.toString(configURL, StandardCharsets.UTF_8);
////            ConversionResult conversionResult = yamlMapper.readerFor(ConversionResult.class).readValue(yamlCfgFile);
////            // Set Config Databases;
////            Set<String> databases = new TreeSet<>(conversionResult.getDatabases().keySet());
////            config.setDatabases(databases);
////            // Replace the conversion in the session.
////            session.setConversionResult(conversionResult);
//        } catch (UnrecognizedPropertyException upe) {
//            // Appears that the file isn't a Conversion file, so try to load it as a DBMirror file.
//            try {
//                DBMirrorTest dbMirror = loadDBMirrorFromFile(config.getLoadTestDataFile());
//                // Reset the work for debug session.
//                dbMirror.stripWork();
//                ConversionResult conversionResult = new ConversionResult();
//                conversionResult.getDatabases().put(dbMirror.getName(), dbMirror);
//                // Set Config Databases;
//                Set<String> databases = new TreeSet<>(conversionResult.getDatabases().keySet());
//                config.setDatabases(databases);
//                // Replace the conversion in the session.
//                session.setConversionResult(conversionResult);
//            } catch (Throwable t2) {
//                log.error(t2.getMessage(), t2);
//                throw t2;
//            }
//        } catch (Throwable t) {
//            log.error("Issue loading test data", t);
//            throw new RuntimeException(t);
//        }
//    }

    private LegacyConversionWrapper loadDBMirrorFromFile(String filename) {
        LegacyConversionWrapper dbMirror = null;
        try {
            log.info("Reconstituting DBMirror from file: {}", filename);
            log.info("Checking 'classpath' for DBMirror file");
            URL configURL = this.getClass().getResource(filename);
            if (isNull(configURL)) {
                log.info("Checking filesystem for DBMirror file: {}", filename);
                File conversionFile = new File(filename);
                if (!conversionFile.exists()) {
                    log.error("Couldn't locate DBMirror file: {}", filename);
                    throw new RuntimeException("Couldn't locate DBMirror file: " + filename);
                }
                configURL = conversionFile.toURI().toURL();
            }
            String yamlCfgFile = IOUtils.toString(configURL, StandardCharsets.UTF_8);
            dbMirror = yamlMapper.readerFor(LegacyConversionWrapper.class).readValue(yamlCfgFile);
        } catch (UnrecognizedPropertyException upe) {
            log.error("There may have been a breaking change in the configuration since the previous " +
                    "release. Review the note below and remove the 'Unrecognized field' from the configuration and try " +
                    "again.", upe);
            throw new RuntimeException("\nThere may have been a breaking change in the configuration since the previous " +
                    "release. Review the note below and remove the 'Unrecognized field' from the configuration and try " +
                    "again.\n\n", upe);
        } catch (Throwable t) {
            // Look for yaml update errors.
            if (t.toString().contains("MismatchedInputException")) {
                log.error("The format of the 'config' yaml file MAY HAVE CHANGED from the last release.  Please make a copy and run " +
                        "'-su|--setup' again to recreate in the new format", t);
                throw new RuntimeException("The format of the 'config' yaml file MAY HAVE CHANGED from the last release.  Please make a copy and run " +
                        "'-su|--setup' again to recreate in the new format", t);
            } else {
                log.error(t.getMessage(), t);
                throw new RuntimeException("A configuration element is no longer valid, progress.  Please remove the element from the configuration yaml and try again.", t);
            }
        }
        return dbMirror;
    }

    @Bean("cliConversionResult")
    // Needs to happen after all the configs have been set.
//    @Order(15)
    public ConversionResult conversionPostProcessing(HmsMirrorConfig config) {
//        return args -> {
            // Create a session for the CLI.  It will be the 'current' session. prefix 'cli-' with timestamp.
            log.info("Creating Session for CLI");
            DateFormat dtf = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss_SSS");
            String cliSessionId = "cli-" + dtf.format(new Date());

        ConversionResult conversionResult = HmsMirrorConfigConverter.convert(config, "cliSessionId");
            getExecutionContextService().setConversionResult(conversionResult);
            getExecutionContextService().setHmsMirrorConfig(config);
            RunStatus runStatus = conversionResult.getRunStatus();
            getExecutionContextService().setRunStatus(runStatus);

            log.info("Post Processing Conversion");
            if (config.isLoadingTestData()) {
                // Load Test Data.
                conversionResult.setMockTestDataset(Boolean.TRUE);
                LegacyConversionWrapper dbMirrorTest = loadDBMirrorFromFile(config.getLoadTestDataFile());
                getConversionResultService().loadLegacyConversionWrapperForTestData(dbMirrorTest);
            }

//        try {
//            getConversionResultRepository().save(conversionResult);
//        } catch (RepositoryException e) {
//            throw new RuntimeException(e);
//        }
//        try {
//            getRunStatusRepository().save(conversionResult.getKey(), runStatus);
//        } catch (RepositoryException e) {
//            throw new RuntimeException(e);
//        }

        // Remove Tables from Map.
            // TODO: Account for this eventually. I think this is already done in the 'theWork' process.
//            for (DBMirror dbMirror : conversionResult.getDatabases().values()) {
//                dbMirror.getTableMirrors().entrySet().removeIf(entry -> entry.getValue().isRemove());
//            }
//        };

        return conversionResult;
    }


}