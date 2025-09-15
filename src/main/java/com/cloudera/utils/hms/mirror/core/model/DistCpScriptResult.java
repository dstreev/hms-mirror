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

package com.cloudera.utils.hms.mirror.core.model;

import java.util.List;

/**
 * Result of DistCp script generation.
 */
public class DistCpScriptResult {
    private final boolean success;
    private final String scriptContent;
    private final List<String> scriptFiles;
    private final String message;

    public DistCpScriptResult(boolean success, String scriptContent, 
                             List<String> scriptFiles, String message) {
        this.success = success;
        this.scriptContent = scriptContent;
        this.scriptFiles = scriptFiles;
        this.message = message;
    }

    public static DistCpScriptResult success(String scriptContent, List<String> scriptFiles) {
        return new DistCpScriptResult(true, scriptContent, scriptFiles, "Scripts generated successfully");
    }

    public static DistCpScriptResult failure(String message) {
        return new DistCpScriptResult(false, "", List.of(), message);
    }

    public boolean isSuccess() { return success; }
    public String getScriptContent() { return scriptContent; }
    public List<String> getScriptFiles() { return scriptFiles; }
    public String getMessage() { return message; }
}