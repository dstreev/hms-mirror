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

package com.cloudera.utils.hms.mirror.service;

import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.support.Conversion;
import com.cloudera.utils.hms.mirror.domain.support.ExecuteSession;
import com.cloudera.utils.hms.mirror.domain.support.RunStatus;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
@Slf4j
@Getter
public class ExecuteSessionService {

    private final String DEFAULT = "default.yaml";

    private ExecuteSession currentSession;
    private final Map<String, ExecuteSession> sessions = new HashMap<>();

    public ExecuteSession createSession(String sessionId, HmsMirrorConfig hmsMirrorConfig) {
        String sessionName = sessionId != null? sessionId : DEFAULT;

        if (sessions.containsKey(sessionName)) {
            return sessions.get(sessionName);
        } else {
            ExecuteSession session = new ExecuteSession();
            session.setSessionId(sessionName);
            session.setHmsMirrorConfig(hmsMirrorConfig);
            session.setRunStatus(new RunStatus());
            session.setConversion(new Conversion());
            sessions.put(sessionName, session);
            return session;
        }
    }

    // TODO: Handle Session Not Found
    public ExecuteSession getSession(String sessionId) {
        String sessionName = sessionId != null? sessionId : DEFAULT;

        // If the session is the default, create it if it doesn't exist
        if (sessionName.equals(DEFAULT) && !sessions.containsKey(DEFAULT)) {
            return createSession(DEFAULT, null);
        } else {
            return sessions.get(sessionName);
        }
    }

    public void setCurrentSession(String sessionId) {
        currentSession = getSession(sessionId);
    }

    public ExecuteSession getCurrentSession() {
        if (currentSession == null) {
            currentSession = getSession(DEFAULT);
        }
        return currentSession;
    }
}
