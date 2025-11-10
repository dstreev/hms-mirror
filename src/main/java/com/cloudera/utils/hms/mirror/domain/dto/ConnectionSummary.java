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
        // Determine overall test status from individual test results
        String testStatus = determineOverallTestStatus(connectionDto);

        return ConnectionSummary.builder()
                .id(connectionDto.getKey())
                .name(connectionDto.getName())
                .description(connectionDto.getDescription())
                .environment(connectionDto.getEnvironment() != null ? connectionDto.getEnvironment().name() : null)
                .platformType(connectionDto.getPlatformType())
//                .isDefault(connectionDto.isDefault())
                .testStatus(testStatus)
                .created(connectionDto.getCreated())
                .modified(connectionDto.getModified())
                .build();
    }

    private static String determineOverallTestStatus(ConnectionDto connectionDto) {
        // Collect all test results
        java.util.List<ConnectionDto.ConnectionTestResults> allResults = new java.util.ArrayList<>();
        if (connectionDto.getHcfsTestResults() != null) allResults.add(connectionDto.getHcfsTestResults());
        if (connectionDto.getHs2TestResults() != null) allResults.add(connectionDto.getHs2TestResults());
        if (connectionDto.getMetastoreDirectTestResults() != null) allResults.add(connectionDto.getMetastoreDirectTestResults());

        if (allResults.isEmpty()) {
            return "never_tested";
        }

        // If any test failed, overall status is FAILED
        boolean anyFailed = allResults.stream()
                .anyMatch(r -> r.getStatus() == ConnectionDto.ConnectionTestResults.TestStatus.FAILED);
        if (anyFailed) {
            return "failed";
        }

        // If all tests succeeded, overall status is SUCCESS
        boolean allSuccess = allResults.stream()
                .allMatch(r -> r.getStatus() == ConnectionDto.ConnectionTestResults.TestStatus.SUCCESS);
        if (allSuccess) {
            return "success";
        }

        return "never_tested";
    }
}