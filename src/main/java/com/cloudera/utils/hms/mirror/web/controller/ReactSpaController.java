/*
 * Copyright (c) 2024-2025. Cloudera, Inc. All Rights Reserved
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

package com.cloudera.utils.hms.mirror.web.controller;

import com.cloudera.utils.hms.mirror.web.config.UIVersionConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import jakarta.servlet.http.HttpServletRequest;

@Controller
@Order(1) // High priority to override other controllers
@Slf4j
@ConditionalOnProperty(name = "hms-mirror.ui.version", havingValue = "react", matchIfMissing = true)
public class ReactSpaController {
    
    @Autowired
    private UIVersionConfig uiVersionConfig;

    /**
     * Serves the React SPA for the root path.
     */
    @GetMapping(value = {"", "/"}, produces = "text/html")
    public String index(HttpServletRequest request) {
        String requestUri = request.getRequestURI();
        log.info("Serving React SPA for root: {}", requestUri);
        return "forward:/react/index.html";
    }

    /**
     * Serves the React SPA for client-side routes.
     */
    @GetMapping({"/dashboard", "/config", "/config/**", "/connections", "/databases", "/execution", "/reports", "/reports/**"})
    public String spa(HttpServletRequest request) {
        String requestUri = request.getRequestURI();
        log.info("Serving React SPA for client route: {}", requestUri);
        return "forward:/react/index.html";
    }

    /**
     * Redirect legacy Thymeleaf paths to the React SPA.
     */
    @GetMapping({"/legacy/**", "/thymeleaf/**"})
    public String redirectLegacy(HttpServletRequest request) {
        String requestUri = request.getRequestURI();
        log.info("Redirecting legacy path {} to React SPA", requestUri);
        return "redirect:/";
    }
}