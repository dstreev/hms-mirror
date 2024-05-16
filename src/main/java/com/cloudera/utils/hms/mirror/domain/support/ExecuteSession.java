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

import com.cloudera.utils.hadoop.cli.CliEnvironment;
import com.cloudera.utils.hms.mirror.MessageCode;
import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicBoolean;

@Getter
@Setter
@Slf4j
public class ExecuteSession {

    private String sessionId;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private RunStatus runStatus;
    private HmsMirrorConfig hmsMirrorConfig;
    private CliEnvironment cliEnvironment;
    private Conversion conversion;

    public void addError(MessageCode code) {
        getRunStatus().addError(code);
    }

    public void addError(MessageCode code, Object... args) {
        getRunStatus().addError(code, args);
    }

    public void addWarning(MessageCode code) {
        getRunStatus().addWarning(code);
    }

    public void addWarning(MessageCode code, Object... args) {
        getRunStatus().addWarning(code, args);
    }

}
