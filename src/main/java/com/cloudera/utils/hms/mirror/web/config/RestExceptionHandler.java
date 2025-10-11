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

import com.cloudera.utils.hms.mirror.exceptions.EncryptionException;
import com.cloudera.utils.hms.mirror.exceptions.MismatchException;
import com.cloudera.utils.hms.mirror.exceptions.RequiredConfigurationException;
import com.cloudera.utils.hms.mirror.exceptions.SessionException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import jakarta.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.sql.SQLInvalidAuthorizationSpecException;
import java.util.HashMap;
import java.util.Map;

@RestControllerAdvice(basePackages = "com.cloudera.utils.hms.mirror.web.controller")
@Order(1) // Higher priority than MirrorExceptionHandler
@Slf4j
public class RestExceptionHandler {

    @ExceptionHandler(value = SessionException.class)
    public ResponseEntity<Map<String, Object>> sessionExceptionHandler(HttpServletRequest request, SessionException exception) {
        log.error("Session exception in REST API: {}", exception.getMessage(), exception);
        Map<String, Object> errorResponse = new HashMap<>();
        errorResponse.put("error", "Session Exception");
        errorResponse.put("message", exception.getMessage());
        errorResponse.put("path", request.getRequestURI());
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
    }

    @ExceptionHandler(value = RequiredConfigurationException.class)
    public ResponseEntity<Map<String, Object>> reqConfigExceptionHandler(HttpServletRequest request, RequiredConfigurationException exception) {
        log.error("Required configuration exception in REST API: {}", exception.getMessage(), exception);
        Map<String, Object> errorResponse = new HashMap<>();
        errorResponse.put("error", "Required Configuration");
        errorResponse.put("message", exception.getMessage());
        errorResponse.put("path", request.getRequestURI());
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(errorResponse);
    }

    @ExceptionHandler(value = EncryptionException.class)
    public ResponseEntity<Map<String, Object>> encryptionExceptionHandler(HttpServletRequest request, EncryptionException exception) {
        log.error("Encryption exception in REST API: {}", exception.getMessage(), exception);
        Map<String, Object> errorResponse = new HashMap<>();
        errorResponse.put("error", "Password Encryption/Decryption Issue");
        errorResponse.put("message", exception.getMessage());
        errorResponse.put("path", request.getRequestURI());
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
    }

    @ExceptionHandler(value = MismatchException.class)
    public ResponseEntity<Map<String, Object>> misMatchExceptionHandler(HttpServletRequest request, MismatchException exception) {
        log.error("Mismatch exception in REST API: {}", exception.getMessage(), exception);
        Map<String, Object> errorResponse = new HashMap<>();
        errorResponse.put("error", "Mismatch Issue");
        errorResponse.put("message", exception.getMessage());
        errorResponse.put("path", request.getRequestURI());
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(errorResponse);
    }

    @ExceptionHandler(value = IOException.class)
    public ResponseEntity<Map<String, Object>> ioExceptionHandler(HttpServletRequest request, IOException exception) {
        log.error("IO exception in REST API: {}", exception.getMessage(), exception);
        Map<String, Object> errorResponse = new HashMap<>();
        errorResponse.put("error", "IO Exception Issue");
        errorResponse.put("message", exception.getMessage());
        errorResponse.put("path", request.getRequestURI());
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
    }

    @ExceptionHandler(value = UnknownHostException.class)
    public ResponseEntity<Map<String, Object>> unKnownHostHandler(HttpServletRequest request, UnknownHostException exception) {
        log.error("Unknown host exception in REST API: {}", exception.getMessage(), exception);
        Map<String, Object> errorResponse = new HashMap<>();
        errorResponse.put("error", "Unknown Host Issue");
        errorResponse.put("message", exception.getMessage());
        errorResponse.put("path", request.getRequestURI());
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(errorResponse);
    }

    @ExceptionHandler(value = SQLException.class)
    public ResponseEntity<Map<String, Object>> sqlExceptionHandler(HttpServletRequest request, SQLException exception) {
        log.error("SQL exception in REST API: {}", exception.getMessage(), exception);
        Map<String, Object> errorResponse = new HashMap<>();
        errorResponse.put("error", "SQL Exception Issue");
        errorResponse.put("message", exception.getMessage());
        errorResponse.put("path", request.getRequestURI());
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
    }

    @ExceptionHandler(value = SQLInvalidAuthorizationSpecException.class)
    public ResponseEntity<Map<String, Object>> SQLInvalidAuthorizationSpecExceptionHandler(HttpServletRequest request, SQLInvalidAuthorizationSpecException exception) {
        log.error("SQL invalid auth exception in REST API: {}", exception.getMessage(), exception);
        Map<String, Object> errorResponse = new HashMap<>();
        errorResponse.put("error", "SQL Invalid Auth Exception Issue");
        errorResponse.put("message", exception.getMessage());
        errorResponse.put("path", request.getRequestURI());
        return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(errorResponse);
    }

    @ExceptionHandler(value = IllegalStateException.class)
    public ResponseEntity<Map<String, Object>> illegalStateExceptionHandler(HttpServletRequest request, IllegalStateException exception) {
        log.error("Illegal state exception in REST API: {}", exception.getMessage(), exception);
        Map<String, Object> errorResponse = new HashMap<>();
        errorResponse.put("error", "Internal Server Error");
        errorResponse.put("message", "An internal error occurred while processing the request");
        errorResponse.put("path", request.getRequestURI());
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
    }

    @ExceptionHandler(value = Exception.class)
    public ResponseEntity<Map<String, Object>> generalExceptionHandler(HttpServletRequest request, Exception exception) {
        log.error("General exception in REST API: {}", exception.getMessage(), exception);
        Map<String, Object> errorResponse = new HashMap<>();
        errorResponse.put("error", "Internal Server Error");
        errorResponse.put("message", "An unexpected error occurred");
        errorResponse.put("path", request.getRequestURI());
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
    }
}