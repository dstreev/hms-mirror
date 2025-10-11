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

package com.cloudera.utils.hms.mirror.web.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.web.servlet.error.ErrorController;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

import jakarta.servlet.RequestDispatcher;
import jakarta.servlet.http.HttpServletRequest;
import java.util.HashMap;
import java.util.Map;

@Controller
@Slf4j
public class ApiErrorController implements ErrorController {

    @RequestMapping("/error")
    public Object handleError(HttpServletRequest request) {
        String requestUri = (String) request.getAttribute(RequestDispatcher.FORWARD_REQUEST_URI);
        
        // Check if this is an API request
        if (requestUri != null && requestUri.startsWith("/hms-mirror/api/")) {
            log.debug("Handling API error for URI: {}", requestUri);
            
            Object status = request.getAttribute(RequestDispatcher.ERROR_STATUS_CODE);
            Integer statusCode = status != null ? (Integer) status : 500;
            
            Object exception = request.getAttribute(RequestDispatcher.ERROR_EXCEPTION);
            String errorMessage = "Internal server error";
            
            if (exception instanceof Exception) {
                Exception ex = (Exception) exception;
                errorMessage = ex.getMessage();
                log.error("API error for {}: {}", requestUri, errorMessage, ex);
            }
            
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Internal Server Error");
            errorResponse.put("message", errorMessage);
            errorResponse.put("path", requestUri);
            errorResponse.put("status", statusCode);
            
            return ResponseEntity
                .status(HttpStatus.valueOf(statusCode))
                .contentType(MediaType.APPLICATION_JSON)
                .body(errorResponse);
        }
        
        // For non-API requests, forward to default error page
        return "forward:/error.html";
    }
}