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

package com.cloudera.utils.hms.mirror.web.controller;

import com.cloudera.utils.hms.mirror.web.config.UIVersionConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

import jakarta.servlet.http.HttpServletRequest;

@Controller
@Order(1) // High priority to override other controllers
@Slf4j
@ConditionalOnProperty(name = "hms-mirror.ui.version", havingValue = "thymeleaf")
public class LegacyUIController {
    
    @Autowired
    private UIVersionConfig uiVersionConfig;

    /**
     * When using Thymeleaf UI, redirect root to the legacy home page.
     */
    @GetMapping(value = {"", "/"})
    public String root(HttpServletRequest request) {
        String requestUri = request.getRequestURI();
        log.info("Redirecting to Thymeleaf UI from: {}", requestUri);
        return "redirect:/config/home";
    }
    
    /**
     * Block React routes when using Thymeleaf UI.
     */
    @GetMapping({"/dashboard", "/config", "/connections", "/databases", "/execution", "/reports"})
    public String blockReactRoutes(HttpServletRequest request) {
        String requestUri = request.getRequestURI();
        log.info("React route {} not available in Thymeleaf mode, redirecting to home", requestUri);
        return "redirect:/config/home";
    }
}