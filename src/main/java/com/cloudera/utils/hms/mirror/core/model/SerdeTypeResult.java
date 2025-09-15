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

import com.cloudera.utils.hms.mirror.domain.support.SerdeType;

/**
 * Result of SerDe type determination.
 */
public class SerdeTypeResult {
    private final SerdeType serdeType;
    private final String fileFormat;
    private final boolean detected;
    private final String message;

    public SerdeTypeResult(SerdeType serdeType, String fileFormat, boolean detected, String message) {
        this.serdeType = serdeType;
        this.fileFormat = fileFormat;
        this.detected = detected;
        this.message = message;
    }

    public static SerdeTypeResult detected(SerdeType serdeType, String fileFormat) {
        return new SerdeTypeResult(serdeType, fileFormat, true, "SerDe type detected successfully");
    }

    public static SerdeTypeResult unknown(String fileFormat, String reason) {
        return new SerdeTypeResult(SerdeType.UNKNOWN, fileFormat, false, reason);
    }

    public SerdeType getSerdeType() { return serdeType; }
    public String getFileFormat() { return fileFormat; }
    public boolean isDetected() { return detected; }
    public String getMessage() { return message; }
}