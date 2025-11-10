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
import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

import java.io.IOException;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.sql.SQLInvalidAuthorizationSpecException;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * Global exception handler for the application.
 * Returns JSON error responses for all exceptions to support the React Web UI.
 */
@ControllerAdvice
@Slf4j
public class MirrorExceptionHandler {

    /**
     * Creates a standard error response map with timestamp, error details, and request path.
     */
    private Map<String, Object> createErrorResponse(String type, String message, String path) {
        Map<String, Object> errorResponse = new HashMap<>();
        errorResponse.put("timestamp", LocalDateTime.now().toString());
        errorResponse.put("error", type);
        errorResponse.put("message", message);
        errorResponse.put("path", path);
        return errorResponse;
    }

    @ExceptionHandler(value = SessionException.class)
    public ResponseEntity<Map<String, Object>> sessionExceptionHandler(HttpServletRequest request, SessionException exception) {
        log.error("Session exception occurred: {}", exception.getMessage(), exception);
        Map<String, Object> errorResponse = createErrorResponse(
            "Session Exception",
            exception.getMessage(),
            request.getRequestURI()
        );
        return ResponseEntity
            .status(HttpStatus.BAD_REQUEST)
            .contentType(MediaType.APPLICATION_JSON)
            .body(errorResponse);
    }

    @ExceptionHandler(value = RequiredConfigurationException.class)
    public ResponseEntity<Map<String, Object>> reqConfigExceptionHandler(HttpServletRequest request, RequiredConfigurationException exception) {
        log.error("Required configuration exception: {}", exception.getMessage(), exception);
        Map<String, Object> errorResponse = createErrorResponse(
            "Required Configuration",
            exception.getMessage(),
            request.getRequestURI()
        );
        return ResponseEntity
            .status(HttpStatus.BAD_REQUEST)
            .contentType(MediaType.APPLICATION_JSON)
            .body(errorResponse);
    }

    @ExceptionHandler(value = EncryptionException.class)
    public ResponseEntity<Map<String, Object>> encryptionExceptionHandler(HttpServletRequest request, EncryptionException exception) {
        log.error("Encryption/Decryption exception: {}", exception.getMessage(), exception);
        Map<String, Object> errorResponse = createErrorResponse(
            "Password Encryption/Decryption Issue",
            exception.getMessage(),
            request.getRequestURI()
        );
        return ResponseEntity
            .status(HttpStatus.INTERNAL_SERVER_ERROR)
            .contentType(MediaType.APPLICATION_JSON)
            .body(errorResponse);
    }

    @ExceptionHandler(value = MismatchException.class)
    public ResponseEntity<Map<String, Object>> misMatchExceptionHandler(HttpServletRequest request, MismatchException exception) {
        log.error("Mismatch exception: {}", exception.getMessage(), exception);
        Map<String, Object> errorResponse = createErrorResponse(
            "Mismatch Issue",
            exception.getMessage(),
            request.getRequestURI()
        );
        return ResponseEntity
            .status(HttpStatus.BAD_REQUEST)
            .contentType(MediaType.APPLICATION_JSON)
            .body(errorResponse);
    }

    @ExceptionHandler(value = IOException.class)
    public ResponseEntity<Map<String, Object>> ioExceptionHandler(HttpServletRequest request, IOException exception) {
        log.error("IO exception: {}", exception.getMessage(), exception);
        Map<String, Object> errorResponse = createErrorResponse(
            "IO Exception Issue",
            exception.getMessage(),
            request.getRequestURI()
        );
        return ResponseEntity
            .status(HttpStatus.INTERNAL_SERVER_ERROR)
            .contentType(MediaType.APPLICATION_JSON)
            .body(errorResponse);
    }

    @ExceptionHandler(value = UnknownHostException.class)
    public ResponseEntity<Map<String, Object>> unKnownHostHandler(HttpServletRequest request, UnknownHostException exception) {
        log.error("Unknown host exception: {}", exception.getMessage(), exception);
        Map<String, Object> errorResponse = createErrorResponse(
            "Unknown Host Issue",
            exception.getMessage(),
            request.getRequestURI()
        );
        return ResponseEntity
            .status(HttpStatus.BAD_REQUEST)
            .contentType(MediaType.APPLICATION_JSON)
            .body(errorResponse);
    }

    @ExceptionHandler(value = SQLException.class)
    public ResponseEntity<Map<String, Object>> sqlExceptionHandler(HttpServletRequest request, SQLException exception) {
        log.error("SQL exception: {}", exception.getMessage(), exception);
        Map<String, Object> errorResponse = createErrorResponse(
            "SQL Exception Issue",
            exception.getMessage(),
            request.getRequestURI()
        );
        return ResponseEntity
            .status(HttpStatus.INTERNAL_SERVER_ERROR)
            .contentType(MediaType.APPLICATION_JSON)
            .body(errorResponse);
    }

    @ExceptionHandler(value = SQLInvalidAuthorizationSpecException.class)
    public ResponseEntity<Map<String, Object>> sqlInvalidAuthorizationSpecExceptionHandler(HttpServletRequest request, SQLInvalidAuthorizationSpecException exception) {
        log.error("SQL invalid authorization exception: {}", exception.getMessage(), exception);
        Map<String, Object> errorResponse = createErrorResponse(
            "SQL Invalid Auth Exception Issue",
            exception.getMessage(),
            request.getRequestURI()
        );
        return ResponseEntity
            .status(HttpStatus.UNAUTHORIZED)
            .contentType(MediaType.APPLICATION_JSON)
            .body(errorResponse);
    }

    @ExceptionHandler(value = IllegalStateException.class)
    public ResponseEntity<Map<String, Object>> illegalStateExceptionHandler(HttpServletRequest request, IllegalStateException exception) {
        log.error("Illegal state exception: {}", exception.getMessage(), exception);
        Map<String, Object> errorResponse = createErrorResponse(
            "Internal Server Error",
            "An internal error occurred while processing the request",
            request.getRequestURI()
        );
        return ResponseEntity
            .status(HttpStatus.INTERNAL_SERVER_ERROR)
            .contentType(MediaType.APPLICATION_JSON)
            .body(errorResponse);
    }

    /**
     * Catch-all exception handler for any unhandled exceptions.
     */
    @ExceptionHandler(value = Exception.class)
    public ResponseEntity<Map<String, Object>> generalExceptionHandler(HttpServletRequest request, Exception exception) {
        log.error("Unhandled exception: {}", exception.getMessage(), exception);
        Map<String, Object> errorResponse = createErrorResponse(
            "Internal Server Error",
            "An unexpected error occurred: " + exception.getMessage(),
            request.getRequestURI()
        );
        return ResponseEntity
            .status(HttpStatus.INTERNAL_SERVER_ERROR)
            .contentType(MediaType.APPLICATION_JSON)
            .body(errorResponse);
    }
}