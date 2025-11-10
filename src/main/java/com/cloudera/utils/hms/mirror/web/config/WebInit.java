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

package com.cloudera.utils.hms.mirror.web.config;

import com.cloudera.utils.hms.mirror.service.DomainService;
import com.cloudera.utils.hms.mirror.service.ExecuteSessionService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.FileSystems;

@Configuration
@Slf4j
public class WebInit {

    private final DomainService domainService;
    private final ExecuteSessionService executeSessionService;

    /**
     * Constructs the web initializer with the given services.
     *
     * @param domainService           Service for domain operations and configuration deserialization
     * @param executeSessionService   Service for session management and report output directory setup
     */
    public WebInit(DomainService domainService, ExecuteSessionService executeSessionService) {
        this.domainService = domainService;
        this.executeSessionService = executeSessionService;
    }

    /**
     * Configures the report output directory to a default path if the property is not set.
     * Uses the user's home directory as the default location.
     *
     * @return CommandLineRunner that sets up and validates the directory
     */
    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.output-dir",
            havingValue = "false")
    CommandLineRunner configOutputDirNotSet() {
        String value = System.getProperty("user.home") + File.separator + ".hms-mirror/reports";
        return configOutputDirInternal(value);
    }

    /**
     * Internal helper to validate and set up the output directory for reports.
     * Ensures the directory exists, attempts to write a test file to guarantee write permission.
     * Logs an error if the directory cannot be written to.
     *
     * @param value Directory path to set up
     * @return CommandLineRunner that performs the setup and validation
     */
    CommandLineRunner configOutputDirInternal(String value) {
        return args -> {
            log.info("output-dir: {}", value);
            executeSessionService.setReportOutputDirectory(value, true);
            File reportPathDir = new File(value);
            if (!reportPathDir.exists()) {
                reportPathDir.mkdirs();
            }
            File testFile = new File(value + FileSystems.getDefault().getSeparator() + ".dir-check");
            // Test file to ensure we can write to it for the report.
            try {
                new FileOutputStream(testFile).close();
            } catch (IOException e) {
                log.error("Can't write to report output directory: " + value, e);
            }
        };
    }
}