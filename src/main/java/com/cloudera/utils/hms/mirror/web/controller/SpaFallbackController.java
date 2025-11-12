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

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

/**
 * SPA Fallback Controller - Routes all non-API paths to React index.html
 * This enables React Router to handle client-side routing
 *
 * Uses LOWEST_PRECEDENCE to ensure REST controllers and resource handlers
 * are matched first before falling back to the SPA
 */
@Controller
@ConditionalOnProperty(name = "hms-mirror.ui.version", havingValue = "react", matchIfMissing = true)
@Order(Ordered.LOWEST_PRECEDENCE)
public class SpaFallbackController {

    /**
     * Forward all non-API routes to React index.html
     * This allows React Router to handle client-side navigation
     *
     * IMPORTANT: When context-path is /hms-mirror, Spring strips that prefix
     * before matching these paths. So we match /jobs/** not /hms-mirror/jobs/**
     *
     * Excludes (handled by other controllers/handlers):
     * - /api/** (REST API endpoints)
     * - /actuator/** (Spring Boot Actuator)
     * - /static/** (static resources)
     * - /manifest.json (PWA manifest)
     * - /favicon.ico (favicon)
     */
    @GetMapping(value = {
            "/",
            "/connections",
            "/connections/**",
            "/datasets",
            "/datasets/**",
            "/config",
            "/config/**",
            "/jobs",
            "/jobs/**",
            "/execution",
            "/execution/**",
            "/summary",
            "/reports",
            "/reports/**",
            "/rocksdb",
            "/rocksdb/**",
            "/encryption"
    })
    public String forward() {
        // Forward to React's index.html
        // The /hms-mirror prefix is added automatically by context-path
        return "forward:/index.html";
    }
}
