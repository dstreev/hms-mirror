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
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@Slf4j
public class MirrorConsoleApp implements CommandLineRunner {

    public static void main(String[] args) {
        log.info("STARTING THE APPLICATION");
        SpringApplication.run(MirrorConsoleApp.class, args);
        log.info("APPLICATION FINISHED");
    }

    @Override
    public void run(String... args) {
        Mirror mirror = new Mirror();
        System.exit((int) mirror.go(args));
//        log.info("EXECUTING : command line runner");
//
//        for (int i = 0; i < args.length; ++i) {
//            log.info("args[{}]: {}", i, args[i]);
//        }
    }
}
