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

import com.cloudera.utils.hadoop.hms.mirror.Config;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableAsync
@Slf4j
public class MirrorWebApp
//        implements ApplicationRunner
{

    public static void main(String[] args) {
        log.info("STARTING THE APPLICATION");
        SpringApplication.run(MirrorWebApp.class, args);
        log.info("APPLICATION FINISHED");
    }

//    @Override
//    public void run(ApplicationArguments args) throws Exception {
//        log.info("EXECUTING : command line runner");
//        for (String arg : args.getSourceArgs()) {
//            log.info("Application Argument: " + arg);
//        }
//        Mirror mirror = new Mirror();
//        int returnCode = (int) mirror.go(args.getSourceArgs());
//        log.info("Return Code: " + returnCode);
//        log.info("APPLICATION FINISHED");
//
//        while (true) {
//            Thread.sleep(10000);
//        }
//    }
}
