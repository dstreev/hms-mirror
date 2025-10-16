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

package com.cloudera.utils.hms.mirror.web.controller.api.v1;

import com.cloudera.utils.hms.mirror.domain.support.PlatformType;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@CrossOrigin
@RestController
@Slf4j
@RequestMapping(path = "/api/v1/app")
@Tag(name = "Application Information", description = "Application metadata and version information")
public class ApplicationInfoController {

    @Value("${hms-mirror.version}")
    private String applicationVersion;

    @Operation(summary = "Get application version")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Application version retrieved successfully")
    })
    @GetMapping("/version")
    public ResponseEntity<Map<String, String>> getVersion() {
        Map<String, String> versionInfo = new HashMap<>();
        versionInfo.put("version", applicationVersion);
        log.debug("Returning application version: {}", applicationVersion);
        return ResponseEntity.ok(versionInfo);
    }

    @Operation(summary = "Get application information")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Application information retrieved successfully")
    })
    @GetMapping("/info")
    public ResponseEntity<Map<String, String>> getInfo() {
        Map<String, String> appInfo = new HashMap<>();
        appInfo.put("name", "HMS-Mirror");
        appInfo.put("version", applicationVersion);
        appInfo.put("description", "Hive Metastore Migration Utility");
        log.debug("Returning application info");
        return ResponseEntity.ok(appInfo);
    }

    @Operation(summary = "Get available platform types")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Platform types retrieved successfully")
    })
    @GetMapping("/platform-types")
    public ResponseEntity<List<String>> getPlatformTypes() {
        List<String> platformTypes = Arrays.stream(PlatformType.values())
                .map(Enum::name)
                .collect(Collectors.toList());
        log.debug("Returning {} platform types", platformTypes.size());
        return ResponseEntity.ok(platformTypes);
    }
}
