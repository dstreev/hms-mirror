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

package com.cloudera.utils.hms.mirror.domain.dto;

import com.cloudera.utils.hms.mirror.domain.support.PlatformType;
import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Builder;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@Builder
public class ConnectionSummary {
    private String id;
    private String name;
    private String description;
    private String environment;
    private PlatformType platformType;
    private boolean isDefault;
    private String testStatus;
    
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private LocalDateTime created;
    
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private LocalDateTime modified;

    public static ConnectionSummary fromConnection(ConnectionDto connectionDto) {
        String testStatus = "never_tested";
        if (connectionDto.getTestResults() != null && connectionDto.getTestResults().getStatus() != null) {
            testStatus = connectionDto.getTestResults().getStatus().name().toLowerCase();
        }

        return ConnectionSummary.builder()
                .id(connectionDto.getId())
                .name(connectionDto.getName())
                .description(connectionDto.getDescription())
                .environment(connectionDto.getEnvironment() != null ? connectionDto.getEnvironment().name() : null)
                .platformType(connectionDto.getPlatformType())
                .isDefault(connectionDto.isDefault())
                .testStatus(testStatus)
                .created(connectionDto.getCreated())
                .modified(connectionDto.getModified())
                .build();
    }
}