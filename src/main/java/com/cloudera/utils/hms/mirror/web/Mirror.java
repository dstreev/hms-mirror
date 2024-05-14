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

package com.cloudera.utils.hms.mirror.web;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.ComponentScans;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@ComponentScans({
        // For the Hadoop CLI Interface
        @ComponentScan(basePackages = "com.cloudera.utils.hadoop")
        , @ComponentScan(basePackages = "com.cloudera.utils.hms.mirror.web")
        , @ComponentScan(basePackages = "com.cloudera.utils.hms.mirror.datastrategy")
        , @ComponentScan(basePackages = "com.cloudera.utils.hms.mirror.service")
        , @ComponentScan(basePackages = "com.cloudera.utils.hms.mirror.util")
})
@EnableAsync
@Slf4j
public class Mirror {

    public static void main(String[] args) {
        log.info("hms-mirror web service Starting...");

        ConfigurableApplicationContext applicationContext = SpringApplication.run(Mirror.class, args);

        log.info("hms-mirror startup Complete.");
    }

    @Bean
    public OpenAPI customOpenAPI(@Value("${hms-mirror.api.version}") String appVersion) {
        return new OpenAPI().info(new Info().title("HMS-Mirror API")
                .version(appVersion)
                .description("This is the HMS-Mirror REST API documentation made with Springdoc OpenAPI 3.0")
//                .termsOfService("http://swagger.io/terms/")
                .license(new License().name("Apache License 2.0")
                        .url("https://spdx.org/licenses/Apache-2.0.html")));
    }

}
