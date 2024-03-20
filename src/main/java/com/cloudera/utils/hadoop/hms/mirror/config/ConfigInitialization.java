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

package com.cloudera.utils.hadoop.hms.mirror.config;

import com.cloudera.utils.hadoop.hms.mirror.Cluster;
import com.cloudera.utils.hadoop.hms.mirror.Config;
import com.cloudera.utils.hadoop.hms.mirror.Progression;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;

@Configuration
@Slf4j
class ConfigInitialization {

    private Config initializeConfig(Progression progression, String configFilename) {
        Config config = null;
        log.info("Initializing Config.");
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        try {
            File cfgFile = new File(configFilename);
            if (cfgFile.exists()) {
                URL cfgUrl = cfgFile.toURI().toURL();

                String yamlCfgFile = IOUtils.toString(cfgUrl, StandardCharsets.UTF_8);
                config = mapper.readerFor(Config.class).readValue(yamlCfgFile);
                config.setProgression(progression);
                // Link the translator to the config
                config.getTranslator().setConfig(config);
                for (Cluster cluster : config.getClusters().values()) {
                    cluster.setConfig(config);
                }
                config.setConfigFilename(configFilename);
//                mapper.readerForUpdating(config).readValue(yamlCfgFile);
            } else {
                throw new RuntimeException("Config file not found: " + configFilename);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        log.info("Config loaded.");
        log.info("Transfer Concurrency: " + config.getTransfer().getConcurrency());
        return config;
    }

    @Bean("config")
    @Order(1)
    public Config loadConfig(Progression progression, @Value("${hms-mirror.config-filename}") String configFilename) {
        return initializeConfig(progression, configFilename);
    }

}
