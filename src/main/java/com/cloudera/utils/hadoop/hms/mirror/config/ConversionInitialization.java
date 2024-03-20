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

import com.cloudera.utils.hadoop.hms.mirror.Config;
import com.cloudera.utils.hadoop.hms.mirror.Conversion;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;

@Configuration
@Slf4j
@Getter
@Setter
class ConversionInitialization {

    @Bean(name = "conversion")
    @Order(10)
    @ConditionalOnProperty(
            name = "hms-mirror.conversion.test-filename",
            havingValue = "false")
    public Conversion buildConversion() {
        log.info("Building Clean Conversion Instance");
        return new Conversion();
    }

    @Bean(name = "conversion")
    @Order(10)
    @ConditionalOnProperty(
            name = "hms-mirror.conversion.test-filename")
    public Conversion loadTestData(Config config, @Value("${hms-mirror.conversion.test-filename}") String filename) throws IOException {
        Conversion conversion = null;
        log.info("Reconstituting Conversion from test data file: " + filename);
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
            conversion = mapper.readerFor(Conversion.class).readValue(yamlCfgFile);
            // Set Config Databases;
            config.setDatabases(conversion.getDatabases().keySet().toArray(new String[0]));
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
        return conversion;
    }

}
