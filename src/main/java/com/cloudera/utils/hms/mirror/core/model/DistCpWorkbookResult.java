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

/**
 * Result of DistCp workbook generation.
 */
public class DistCpWorkbookResult {
    private final boolean success;
    private final String workbookContent;
    private final String workbookFile;
    private final String message;

    public DistCpWorkbookResult(boolean success, String workbookContent, 
                               String workbookFile, String message) {
        this.success = success;
        this.workbookContent = workbookContent;
        this.workbookFile = workbookFile;
        this.message = message;
    }

    public static DistCpWorkbookResult success(String workbookContent, String workbookFile) {
        return new DistCpWorkbookResult(true, workbookContent, workbookFile, "Workbook generated successfully");
    }

    public static DistCpWorkbookResult failure(String message) {
        return new DistCpWorkbookResult(false, "", "", message);
    }

    public boolean isSuccess() { return success; }
    public String getWorkbookContent() { return workbookContent; }
    public String getWorkbookFile() { return workbookFile; }
    public String getMessage() { return message; }
}