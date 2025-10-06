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

public class SessionContextHolder {
    
    private static final ThreadLocal<ExecuteSession> sessionContext = new ThreadLocal<>();
    
    public static void setSession(ExecuteSession session) {
        sessionContext.set(session);
    }
    
    public static ExecuteSession getSession() {
        return sessionContext.get();
    }
    
    public static void clearSession() {
        sessionContext.remove();
    }
    
    public static boolean hasSession() {
        return sessionContext.get() != null;
    }
}