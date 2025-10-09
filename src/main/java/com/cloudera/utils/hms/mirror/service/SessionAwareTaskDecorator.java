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

package com.cloudera.utils.hms.mirror.service;

import com.cloudera.utils.hms.mirror.domain.support.ExecuteSession;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.task.TaskDecorator;

/**
 * Task decorator that ensures ExecuteSession context is propagated to child threads.
 * This is essential for maintaining session context across async operations.
 */
@Slf4j
public class SessionAwareTaskDecorator implements TaskDecorator {

    @Override
    public Runnable decorate(Runnable runnable) {
        // Capture the current session from the calling thread
        ExecuteSession currentSession = SessionContextHolder.getSession();
        
        log.debug("Decorating task with session: {}", 
                 currentSession != null ? currentSession.getSessionId() : "null");
        
        return () -> {
            // Set the captured session in the new thread
            ExecuteSession previousSession = SessionContextHolder.getSession();
            try {
                if (currentSession != null) {
                    SessionContextHolder.setSession(currentSession);
                    log.trace("Set session {} in child thread", currentSession.getSessionId());
                    ExecuteSession childSession = SessionContextHolder.getSession();
                    log.trace("Child thread session is now: {}",
                              childSession != null ? childSession.getSessionId() : "null");
                }
                // Execute the original task
                runnable.run();
            } finally {
                // Restore the previous session (or clear if none)
                if (previousSession != null) {
                    SessionContextHolder.setSession(previousSession);
                } else {
                    SessionContextHolder.clearSession();
                }
                log.trace("Restored session context in child thread");
            }
        };
    }
}