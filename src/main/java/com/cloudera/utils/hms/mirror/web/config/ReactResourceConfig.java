/*
 * Copyright (c) 2024 Cloudera, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.utils.hms.mirror.web.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
@ConditionalOnProperty(name = "hms-mirror.ui.version", havingValue = "react", matchIfMissing = true)
public class ReactResourceConfig implements WebMvcConfigurer {

    @Override
    public void addResourceHandlers(ResourceHandlerRegistry registry) {
        // IMPORTANT: context-path is /hms-mirror, so these paths are relative to that
        // When user requests /hms-mirror/index.html, Spring sees /index.html

        // Serve React app root files (index.html, manifest.json, favicon.ico)
        registry.addResourceHandler("/index.html")
                .addResourceLocations("classpath:/static/react/")
                .resourceChain(false);

        registry.addResourceHandler("/manifest.json")
                .addResourceLocations("classpath:/static/react/");

        registry.addResourceHandler("/favicon.ico")
                .addResourceLocations("classpath:/static/react/");

        // Serve React static assets (JS, CSS, images)
        registry.addResourceHandler("/static/**")
                .addResourceLocations("classpath:/static/react/static/");

        // Legacy /react/** path for backward compatibility
        registry.addResourceHandler("/react/**")
                .addResourceLocations("classpath:/static/react/");
    }
}