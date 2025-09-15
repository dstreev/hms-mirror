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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
@ConditionalOnProperty(name = "hms-mirror.ui.version", havingValue = "react", matchIfMissing = true)
public class ReactResourceConfig implements WebMvcConfigurer {

    @Override
    public void addResourceHandlers(ResourceHandlerRegistry registry) {
        // Serve React static files from /react directory but at root paths
        // This maps /static/** requests to /static/react/static/**
        registry.addResourceHandler("/static/**")
                .addResourceLocations("classpath:/static/react/static/");
        
        // Map /manifest.json to /react/manifest.json
        registry.addResourceHandler("/manifest.json")
                .addResourceLocations("classpath:/static/react/");
        
        // Map /react/** to serve the actual files from /static/react/**
        registry.addResourceHandler("/react/**")
                .addResourceLocations("classpath:/static/react/");
    }
}