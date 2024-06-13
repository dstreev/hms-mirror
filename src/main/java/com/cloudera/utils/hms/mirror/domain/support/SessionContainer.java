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

package com.cloudera.utils.hms.mirror.domain.support;

import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import lombok.Getter;
import lombok.Setter;

import java.util.Set;
import java.util.TreeSet;

@Getter
@Setter
public class SessionContainer {
    private boolean saveAsDefault = Boolean.FALSE;
    private boolean stripMappings = Boolean.TRUE;
    private HmsMirrorConfig config;
    private RunStatus runStatus = null;
    private String sessionId = null;
    private boolean readOnly = Boolean.FALSE;

    public void loadFromSession(ExecuteSession executeSession) {
        if (executeSession != null) {
            this.sessionId = executeSession.getSessionId();
            this.config = executeSession.getResolvedConfig();
            this.runStatus = executeSession.getRunStatus();
        }
    }
}
