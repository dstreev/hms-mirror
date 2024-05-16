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
import com.cloudera.utils.hms.mirror.service.ExecuteSessionService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
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
    public CommandLineRunner initDefaultConfig(@Value("${hms-mirror.config-filename}") String configFilename) {
        return args -> {
            // hms-mirror.config-filename is set in the application.yaml file with the
            //    default location.  It can be overridden by setting the commandline
            //    --hms-mirror.config-filename=<filename>.

            File cfg = new File(configFilename);
            HmsMirrorConfig hmsMirrorConfig;
            if (cfg.exists()) {
                log.info("Loading default config from: " + configFilename);
                hmsMirrorConfig = HmsMirrorConfig.loadConfig(configFilename);
            } else {
                // Return empty config.  This will require the user to setup the config.
                log.warn("No default config found.  Creating empty config.");
                hmsMirrorConfig = new HmsMirrorConfig();
            }
            executeSessionService.createSession(ExecuteSessionService.DEFAULT, hmsMirrorConfig);
        };
    }

}
