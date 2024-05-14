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

package com.cloudera.utils.hms.mirror.web.config;

import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.support.RunStatus;
import com.cloudera.utils.hms.mirror.service.ExecuteSessionService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.File;

@Configuration
@Slf4j
public class WebInit {

    private ExecuteSessionService executeSessionService;

    @Autowired
    public void setExecuteSessionService(ExecuteSessionService executeSessionService) {
        this.executeSessionService = executeSessionService;
    }

    @Bean
    public CommandLineRunner initDefaultConfig() {
        return args -> {
            // Try to find and load the 'default.yaml' config.
            String cfgFile = System.getProperty("user.home")
                    + File.separator + ".hms-mirror/cfg/" + ExecuteSessionService.DEFAULT;
            File cfg = new File(cfgFile);
            if (cfg.exists()) {
                log.info("Loading default config from: " + cfgFile);
                HmsMirrorConfig hmsMirrorConfig = HmsMirrorConfig.loadConfig(cfgFile);
                executeSessionService.createSession(ExecuteSessionService.DEFAULT, hmsMirrorConfig);
            } else {
                // Return empty config.  This will require the user to setup the config.
                log.warn("No default config found.  Creating empty config.");
                HmsMirrorConfig hmsMirrorConfig = new HmsMirrorConfig();
                executeSessionService.createSession(ExecuteSessionService.DEFAULT, hmsMirrorConfig);
            }
        };
    }

    @Bean
    public HmsMirrorConfig buildHmsMirrorConfig() {
        // Try to find and load the 'default.yaml' config.
        String cfgFile = System.getProperty("user.home") + File.separator + ".hms-mirror/cfg/default.yaml";
        File cfg = new File(cfgFile);
        if (cfg.exists()) {
            return HmsMirrorConfig.loadConfig(cfgFile);
        }
        // Return empty config.  This will require the user to setup the config.
        return new HmsMirrorConfig();
    }
//
//    @Bean("runStatus")
//    public RunStatus buildRunStatus() {
//        return new RunStatus();
//    }

}
