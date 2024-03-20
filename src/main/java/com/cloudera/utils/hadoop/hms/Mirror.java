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

package com.cloudera.utils.hadoop.hms;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableAsync
@Slf4j
public class Mirror {

    public static void main(String[] args) {
        // Translate the legacy command line arguments to Spring Boot arguments
        //    before starting the application.
        log.info("Translating command line arguments to Spring Boot arguments");
        CommandLineOptions commandLineOptions = new CommandLineOptions();
        String[] springArgs = commandLineOptions.toSpringBootOption(args);
        log.info("Translated Spring Boot arguments: " + String.join(" ", springArgs));
        log.info("STARTING THE APPLICATION");
        SpringApplication.run(Mirror.class, springArgs);
        log.info("APPLICATION FINISHED");
    }
}
