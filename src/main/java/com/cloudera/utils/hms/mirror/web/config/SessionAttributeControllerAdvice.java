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

package com.cloudera.utils.hms.mirror.web.config;

import com.cloudera.utils.hms.mirror.domain.support.ExecuteSession;
import com.cloudera.utils.hms.mirror.service.SessionManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ModelAttribute;

@ControllerAdvice
@Slf4j
public class SessionAttributeControllerAdvice {

    @Autowired
    private SessionManager sessionManager;

    @ModelAttribute("CURRENT_SESSION")
    public ExecuteSession getCurrentSession() {
        try {
            return sessionManager.getCurrentSession();
        } catch (Exception e) {
            log.debug("Could not retrieve current session: {}", e.getMessage());
            return null;
        }
    }

    @ModelAttribute("SESSION_ID")
    public String getSessionId() {
        try {
            ExecuteSession session = sessionManager.getCurrentSession();
            if (session != null && session.getSessionId() != null) {
                return session.getSessionId();
            }
            log.debug("Session or session ID is null, returning 'unknown'");
            return "unknown";
        } catch (Exception e) {
            log.debug("Could not retrieve session ID: {}", e.getMessage());
            return "unknown";
        }
    }
}