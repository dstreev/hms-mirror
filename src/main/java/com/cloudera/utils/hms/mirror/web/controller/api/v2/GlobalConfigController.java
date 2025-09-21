/*
 * Copyright (c) 2025. Cloudera, Inc. All Rights Reserved
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


package com.cloudera.utils.hms.mirror.web.controller.api.v2;

import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig; // Import top-level domain object
import com.cloudera.utils.hms.mirror.domain.Cluster;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.web.controller.api.v2.service.ConfigurationService;
import com.cloudera.utils.hms.mirror.service.ConfigService;
import com.cloudera.utils.hms.mirror.service.DomainService;
import com.cloudera.utils.hms.mirror.service.PasswordService;
import com.cloudera.utils.hms.mirror.exceptions.EncryptionException;
import com.cloudera.utils.hms.mirror.exceptions.SessionException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * REST Controller for managing top-level (global) HmsMirror configuration properties,
 * and also the 'databases' list.
 * Now directly works with HmsMirrorConfig domain object.
 */
@RestController("GlobalConfigControllerV2")
@RequestMapping("/api/v2/config")
@Tag(name = "Global Configuration", description = "Operations related to top-level HmsMirror configuration properties and the list of databases.")
@Slf4j
public class GlobalConfigController {

    @Autowired
    private ConfigurationService configService;
    
    @Autowired
    private ConfigService legacyConfigService;
    
    @Autowired
    private DomainService domainService;
    
    @Autowired
    private PasswordService passwordService;
    
    @Autowired
    private com.cloudera.utils.hms.mirror.service.ExecuteSessionService executeSessionService;
    
    @Value("${hms-mirror.config.path}")
    private String configPath;
    
    @PostConstruct
    public void init() {
        log.info("GlobalConfigController initialized with config path: {}", configPath);
        if (configPath != null) {
            File configDir = new File(configPath);
            log.info("Config directory exists: {}, is directory: {}", configDir.exists(), configDir.isDirectory());
            if (configDir.exists() && configDir.isDirectory()) {
                File[] files = configDir.listFiles((dir, name) -> name.endsWith(".yaml") || name.endsWith(".yml"));
                log.info("Found {} configuration files in {}", files != null ? files.length : 0, configPath);
            }
        } else {
            log.error("Config path is null after initialization!");
        }
    }

    /**
     * Retrieves the entire current HmsMirror configuration.
     * @return A ResponseEntity containing the complete HmsMirrorConfig domain object.
     */
    @GetMapping(produces = "application/json")
    @Operation(summary = "Get entire HmsMirror configuration",
            description = "Retrieves the complete current HmsMirror configuration.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully retrieved configuration"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<HmsMirrorConfig> getFullConfiguration() {
        log.info("=== GET /api/v2/config called ===");
        // Check if we have a session with config first
        com.cloudera.utils.hms.mirror.domain.support.ExecuteSession session = executeSessionService.getSession();
        log.info("ExecuteSession is null? {}", session == null);
        if (session != null) {
            log.info("Session ID: {}", session.getSessionId());
            log.info("Session config is null? {}", session.getConfig() == null);
        }
        
        HmsMirrorConfig config = null;
        if (session != null && session.getConfig() != null) {
            log.info("Returning configuration from ExecuteSession.");
            config = session.getConfig();
            log.info("ExecuteSession config has clusters? {}", config.getClusters() != null);
            if (config.getClusters() != null) {
                log.info("ExecuteSession config cluster count: {}", config.getClusters().size());
            }
        } else {
            log.info("No ExecuteSession found, returning configuration from ConfigurationService.");
            config = configService.getConfiguration();
            log.info("ConfigurationService config is null? {}", config == null);
            if (config != null) {
                log.info("ConfigurationService config has clusters? {}", config.getClusters() != null);
                if (config.getClusters() != null) {
                    log.info("ConfigurationService config cluster count: {}", config.getClusters().size());
                }
            }
        }
        
        log.info("Final config to return is null? {}", config == null);
        if (config == null) {
            log.warn("Returning null configuration - this will cause 'Configuration Required' page");
            return ResponseEntity.noContent().build();
        }
        return ResponseEntity.ok(config);
    }

    /**
     * Updates top-level HmsMirror configuration properties.
     * Only provided (non-null) fields in the request body will update the existing configuration.
     * @param hmsMirrorConfig A partial HmsMirrorConfig domain object with top-level fields to update.
     * @return A ResponseEntity containing the updated HmsMirrorConfig domain object.
     */
    @PutMapping
    @Operation(summary = "Update top-level HmsMirror configuration properties",
            description = "Updates specific top-level properties of the HmsMirror configuration. " +
                    "Only provided (non-null) fields will be updated. Nested objects and lists require separate endpoints or specific sub-controllers.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully updated configuration"),
            @ApiResponse(responseCode = "400", description = "Invalid request body"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<HmsMirrorConfig> updateGlobalConfiguration(@RequestBody HmsMirrorConfig hmsMirrorConfig) {
        log.info("Received request to update global HmsMirror configuration with data: {}", hmsMirrorConfig);
        HmsMirrorConfig updatedConfig = configService.updateConfiguration(hmsMirrorConfig);
        log.info("Global HmsMirror configuration updated successfully.");
        return ResponseEntity.ok(updatedConfig);
    }

    /*
     * Endpoints for 'databases' set
     */

    /**
     * Retrieves the set of databases configured.
     * @return A ResponseEntity containing a set of database names.
     */
    @GetMapping("/databases")
    @Operation(summary = "Get configured databases",
            description = "Retrieves the set of database names configured in the HmsMirror.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully retrieved set"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<Set<String>> getDatabases() {
        log.info("Received request to get configured databases.");
        Set<String> databases = configService.getDatabases();
        log.info("Returning configured databases with {} entries.", databases.size());
        return ResponseEntity.ok(databases);
    }

    /**
     * Clears all databases from the configuration.
     * @return A ResponseEntity indicating success.
     */
    @DeleteMapping("/databases/clear")
    @Operation(summary = "Clear all configured databases",
            description = "Removes all database entries from the HmsMirror configuration.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Successfully cleared set"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<Void> clearDatabases() {
        log.info("Received request to clear all configured databases.");
        configService.clearDatabases();
        log.info("All configured databases cleared successfully.");
        return ResponseEntity.noContent().build();
    }

    /**
     * Adds a database name to the configuration.
     * @param databaseName The name of the database to add.
     * @return A ResponseEntity indicating success or if the database already exists.
     */
    @PostMapping("/databases")
    @Operation(summary = "Add a database to configuration",
            description = "Adds a new database name to the HmsMirror configuration if it doesn't already exist.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully added database or database already exists"),
            @ApiResponse(responseCode = "400", description = "Invalid request body"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<String> addDatabase(@RequestBody String databaseName) {
        log.info("Received request to add database: {}", databaseName);
        if (configService.addDatabase(databaseName)) {
            log.info("Database '{}' added successfully.", databaseName);
            return ResponseEntity.ok("Database '" + databaseName + "' added.");
        } else {
            log.warn("Database '{}' already exists. Not added.", databaseName);
            return ResponseEntity.ok("Database '" + databaseName + "' already exists.");
        }
    }

    /**
     * Deletes a database name from the configuration.
     * @param databaseName The name of the database to delete.
     * @return A ResponseEntity indicating success or if the database was not found.
     */
    @DeleteMapping("/databases")
    @Operation(summary = "Delete a database from configuration",
            description = "Removes a database name from the HmsMirror configuration if it exists.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully deleted database or database not found"),
            @ApiResponse(responseCode = "400", description = "Invalid request body"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<String> deleteDatabase(@RequestBody String databaseName) {
        log.info("Received request to delete database: {}", databaseName);
        if (configService.deleteDatabase(databaseName)) {
            log.info("Database '{}' deleted successfully.", databaseName);
            return ResponseEntity.ok("Database '" + databaseName + "' deleted.");
        } else {
            log.warn("Database '{}' not found. No deletion performed.", databaseName);
            return ResponseEntity.ok("Database '" + databaseName + "' not found.");
        }
    }

    /**
     * Persists the current configuration to a YAML file.
     * @param persistRequest The request containing the filename to save as.
     * @return A ResponseEntity indicating success or failure.
     */
    @PostMapping(value = "/persist", produces = "application/json", consumes = "application/json")
    @Operation(summary = "Persist configuration to YAML file",
            description = "Saves the current HmsMirror configuration to a YAML file in the config directory.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully persisted configuration"),
            @ApiResponse(responseCode = "400", description = "Invalid request body"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<String> persistConfiguration(@RequestBody PersistConfigRequest persistRequest) {
        log.info("Received request to persist configuration with filename: {}", persistRequest.getFilename());
        
        try {
            HmsMirrorConfig currentConfig = configService.getConfiguration();
            
            String filename = persistRequest.getFilename();
            if (!filename.endsWith(".yaml") && !filename.endsWith(".yml")) {
                filename += ".yaml";
            }
            
            String fullPath = configPath + File.separator + filename;
            boolean success = legacyConfigService.saveConfig(currentConfig, fullPath, true);
            
            if (success) {
                log.info("Configuration persisted successfully to: {}", fullPath);
                return ResponseEntity.ok("Configuration saved successfully to " + filename);
            } else {
                log.error("Failed to persist configuration to: {}", fullPath);
                return ResponseEntity.internalServerError().body("Failed to save configuration");
            }
        } catch (IOException e) {
            log.error("Error persisting configuration: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body("Error saving configuration: " + e.getMessage());
        }
    }

    /**
     * Loads a configuration file by filename and sets it as the current configuration.
     * @param loadRequest The request containing the filename to load.
     * @return A ResponseEntity containing the loaded HmsMirrorConfig.
     */
    @PostMapping(value = "/load", produces = "application/json", consumes = "application/json")
    @Operation(summary = "Load configuration from file",
            description = "Loads a configuration file by filename and sets it as the current in-memory configuration.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully loaded configuration"),
            @ApiResponse(responseCode = "400", description = "Invalid request body or file not found"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<HmsMirrorConfig> loadConfiguration(@RequestBody LoadConfigRequest loadRequest) {
        log.info("Received request to load configuration from file: {}", loadRequest.getFilename());
        
        try {
            String filename = loadRequest.getFilename();
            if (filename == null || filename.trim().isEmpty()) {
                log.error("Filename is required for loading configuration");
                return ResponseEntity.badRequest().build();
            }
            
            // Add .yaml extension if not present
            if (!filename.endsWith(".yaml") && !filename.endsWith(".yml")) {
                filename += ".yaml";
            }
            
            // Load the configuration from file using just the filename (not full path)
            // The ConfigService.loadConfig method handles the path resolution internally
            HmsMirrorConfig loadedConfig = legacyConfigService.loadConfig(filename);
            
            if (loadedConfig == null) {
                log.error("Failed to load configuration from file: {}", filename);
                return ResponseEntity.badRequest().body(null);
            }
            
            // Close any existing session first
            try {
                executeSessionService.closeSession();
            } catch (SessionException e) {
                log.warn("Error closing existing session: {}", e.getMessage());
            }
            
            // Create and set a new ExecuteSession as the single source of truth
            String sessionId = filename != null ? filename.replace(".yaml", "").replace(".yml", "") : "loaded-config";
            com.cloudera.utils.hms.mirror.domain.support.ExecuteSession session = executeSessionService.createSession(sessionId, loadedConfig);
            executeSessionService.setSession(session);
            
            // Also update ConfigurationService to keep it in sync (for any legacy code that might use it)
            configService.replaceConfiguration(loadedConfig);
            
            log.info("Successfully loaded configuration from file: {}", filename);
            return ResponseEntity.ok(loadedConfig);
            
        } catch (Exception e) {
            log.error("Error loading configuration: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(null);
        }
    }

    /**
     * Gets the list of available configuration files.
     * @return A ResponseEntity containing the list of config files.
     */
    @GetMapping("/files")
    @Operation(summary = "Get available configuration files",
            description = "Retrieves the list of available YAML configuration files in the config directory.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully retrieved file list"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<List<String>> getConfigFiles() {
        log.info("Received request to get available configuration files.");
        
        try {
            File configDir = new File(configPath);
            if (!configDir.exists() || !configDir.isDirectory()) {
                log.warn("Config directory does not exist: {}", configPath);
                return ResponseEntity.ok(List.of());
            }
            
            File[] yamlFiles = configDir.listFiles((dir, name) -> 
                name.endsWith(".yaml") || name.endsWith(".yml"));
            
            if (yamlFiles == null) {
                return ResponseEntity.ok(List.of());
            }
            
            List<String> fileNames = List.of(yamlFiles).stream()
                .map(File::getName)
                .sorted()
                .toList();
            
            log.info("Found {} configuration files.", fileNames.size());
            return ResponseEntity.ok(fileNames);
        } catch (Exception e) {
            log.error("Error retrieving configuration files: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(List.of());
        }
    }

    /**
     * Gets metadata for configuration files without loading them into memory.
     * @return A ResponseEntity containing the list of config file metadata.
     */
    @GetMapping("/files/metadata")
    @Operation(summary = "Get configuration file metadata",
            description = "Retrieves metadata for configuration files including data strategy and modification time without loading them into memory.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully retrieved file metadata"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<List<ConfigFileMetadata>> getConfigFilesMetadata() {
        log.info("Received request to get configuration file metadata.");
        
        try {
            File configDir = new File(configPath);
            if (!configDir.exists() || !configDir.isDirectory()) {
                log.warn("Config directory does not exist: {}", configPath);
                return ResponseEntity.ok(List.of());
            }
            
            File[] yamlFiles = configDir.listFiles((dir, name) -> 
                name.endsWith(".yaml") || name.endsWith(".yml"));
            
            if (yamlFiles == null) {
                return ResponseEntity.ok(List.of());
            }
            
            List<ConfigFileMetadata> fileMetadata = List.of(yamlFiles).stream()
                .map(file -> {
                    try {
                        // Read file content to extract data strategy without loading into memory
                        HmsMirrorConfig config = domainService.deserializeConfig(file.getAbsolutePath());
                        
                        return new ConfigFileMetadata(
                            file.getName(),
                            config.getDataStrategy() != null ? config.getDataStrategy().toString() : "Unknown",
                            file.lastModified(),
                            file.length()
                        );
                    } catch (Exception e) {
                        log.warn("Failed to read metadata for file {}: {}", file.getName(), e.getMessage());
                        return new ConfigFileMetadata(
                            file.getName(),
                            "Unknown",
                            file.lastModified(),
                            file.length()
                        );
                    }
                })
                .sorted((a, b) -> a.getFilename().compareTo(b.getFilename()))
                .toList();
            
            log.info("Retrieved metadata for {} configuration files.", fileMetadata.size());
            return ResponseEntity.ok(fileMetadata);
        } catch (Exception e) {
            log.error("Error retrieving configuration file metadata: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(List.of());
        }
    }

    /**
     * Request class for persisting configuration.
     */
    public static class PersistConfigRequest {
        private String filename;

        public String getFilename() {
            return filename;
        }

        public void setFilename(String filename) {
            this.filename = filename;
        }
    }

    /**
     * Request class for loading configuration.
     */
    public static class LoadConfigRequest {
        private String filename;

        public String getFilename() {
            return filename;
        }

        public void setFilename(String filename) {
            this.filename = filename;
        }
    }

    /**
     * Encrypts all passwords in the configuration using the provided password key.
     * @param encryptRequest The request containing the configuration and password key.
     * @return A ResponseEntity containing the configuration with encrypted passwords.
     */
    @PostMapping("/encrypt-passwords")
    @Operation(summary = "Encrypt passwords in configuration",
            description = "Encrypts all passwords in the HmsMirror configuration using the provided password key.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully encrypted passwords"),
            @ApiResponse(responseCode = "400", description = "Invalid request or encryption error"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<HmsMirrorConfig> encryptPasswords(@RequestBody EncryptPasswordsRequest encryptRequest) {
        log.info("Received request to encrypt passwords in configuration.");
        
        if (passwordService == null) {
            log.error("PasswordService is not autowired!");
            return ResponseEntity.internalServerError().body(null);
        }
        
        try {
            if (encryptRequest.getPasswordKey() == null || encryptRequest.getPasswordKey().length() < 6) {
                log.error("Password key must be at least 6 characters.");
                return ResponseEntity.badRequest().body(null);
            }
            
            HmsMirrorConfig config = encryptRequest.getConfig();
            if (config == null) {
                log.error("Configuration is required for encryption.");
                return ResponseEntity.badRequest().body(null);
            }
            
            // Track how many passwords we encrypt
            final int[] encryptedCount = {0}; // Using array to modify in lambda
            
            // Encrypt passwords in HiveServer2 configurations
            if (config.getClusters() != null) {
                log.info("Found {} clusters to process", config.getClusters().size());
                config.getClusters().forEach((env, cluster) -> {
                    log.debug("Processing cluster: {}", env);
                    if (cluster != null && cluster.getHiveServer2() != null && 
                        cluster.getHiveServer2().getConnectionProperties() != null) {
                        
                        // Encrypt password in connection properties
                        String pwd = cluster.getHiveServer2().getConnectionProperties().getProperty("password");
                        log.debug("Cluster {} HiveServer2 has password: {}", env, pwd != null);
                        if (pwd != null && !pwd.isEmpty()) {
                            try {
                                log.debug("Encrypting password for cluster {} HiveServer2", env);
                                String encryptedPwd = passwordService.encryptPassword(encryptRequest.getPasswordKey(), pwd);
                                cluster.getHiveServer2().getConnectionProperties().setProperty("password", encryptedPwd);
                                encryptedCount[0]++;
                                log.info("Successfully encrypted password for cluster {} HiveServer2", env);
                            } catch (EncryptionException e) {
                                log.error("Failed to encrypt HiveServer2 password for cluster {}: {}", env, e.getMessage());
                                throw new RuntimeException("Failed to encrypt passwords", e);
                            }
                        }
                    } else {
                        log.debug("Cluster {} has no HiveServer2 or connection properties", env);
                    }
                    
                    // Encrypt passwords in Metastore Direct configurations
                    if (cluster != null && cluster.getMetastoreDirect() != null && 
                        cluster.getMetastoreDirect().getConnectionProperties() != null) {
                        
                        String pwd = cluster.getMetastoreDirect().getConnectionProperties().getProperty("password");
                        if (pwd != null && !pwd.isEmpty()) {
                            try {
                                String encryptedPwd = passwordService.encryptPassword(encryptRequest.getPasswordKey(), pwd);
                                cluster.getMetastoreDirect().getConnectionProperties().setProperty("password", encryptedPwd);
                                encryptedCount[0]++;
                                log.info("Successfully encrypted password for cluster {} Metastore Direct", env);
                            } catch (EncryptionException e) {
                                log.error("Failed to encrypt Metastore Direct password for cluster {}: {}", env, e.getMessage());
                                throw new RuntimeException("Failed to encrypt passwords", e);
                            }
                        }
                    }
                });
            }
            
            // Mark configuration as having encrypted passwords
            config.setEncryptedPasswords(true);
            config.setPasswordKey(encryptRequest.getPasswordKey());
            
            log.info("Successfully encrypted {} passwords in configuration.", encryptedCount[0]);
            
            if (encryptedCount[0] == 0) {
                log.warn("No passwords were found to encrypt. Make sure passwords are set in connection properties.");
            }
            
            return ResponseEntity.ok(config);
            
        } catch (RuntimeException e) {
            log.error("Runtime error encrypting passwords: {}", e.getMessage(), e);
            if (e.getCause() instanceof EncryptionException) {
                return ResponseEntity.badRequest().body(null);
            }
            return ResponseEntity.internalServerError().body(null);
        } catch (Exception e) {
            log.error("Unexpected error encrypting passwords: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(null);
        }
    }

    /**
     * Checks if the provided password key can successfully decrypt all encrypted passwords.
     * @param checkRequest The request containing the configuration and password key to check.
     * @return A ResponseEntity indicating whether the password key is valid.
     */
    @PostMapping("/check-password-key")
    @Operation(summary = "Check password key validity",
            description = "Verifies if the provided password key can successfully decrypt all encrypted passwords in the configuration.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Password key check completed"),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<Map<String, Object>> checkPasswordKey(@RequestBody CheckPasswordKeyRequest checkRequest) {
        log.info("Received request to check password key validity.");
        
        Map<String, Object> response = new HashMap<>();
        
        try {
            if (checkRequest.getPasswordKey() == null || checkRequest.getPasswordKey().length() < 6) {
                log.error("Password key must be at least 6 characters.");
                response.put("valid", false);
                response.put("message", "Password key must be at least 6 characters.");
                return ResponseEntity.badRequest().body(response);
            }
            
            HmsMirrorConfig config = checkRequest.getConfig();
            if (config == null || !config.isEncryptedPasswords()) {
                log.error("Configuration is not encrypted or is null.");
                response.put("valid", false);
                response.put("message", "Configuration is not encrypted.");
                return ResponseEntity.badRequest().body(response);
            }
            
            // Try to decrypt all passwords to check if the key is valid
            boolean allDecryptedSuccessfully = true;
            List<String> errors = new ArrayList<>();
            
            if (config.getClusters() != null) {
                for (Map.Entry<Environment, Cluster> entry : config.getClusters().entrySet()) {
                    Cluster cluster = entry.getValue();
                    Environment env = entry.getKey();
                    
                    // Check HiveServer2 password
                    if (cluster != null && cluster.getHiveServer2() != null && 
                        cluster.getHiveServer2().getConnectionProperties() != null) {
                        String encryptedPwd = cluster.getHiveServer2().getConnectionProperties().getProperty("password");
                        if (encryptedPwd != null && !encryptedPwd.isEmpty()) {
                            try {
                                passwordService.decryptPassword(checkRequest.getPasswordKey(), encryptedPwd);
                                log.debug("Successfully decrypted HiveServer2 password for cluster {}", env);
                            } catch (EncryptionException e) {
                                allDecryptedSuccessfully = false;
                                errors.add("Failed to decrypt HiveServer2 password for cluster " + env);
                                log.error("Failed to decrypt HiveServer2 password for cluster {}: {}", env, e.getMessage());
                            } catch (Exception e) {
                                allDecryptedSuccessfully = false;
                                errors.add("Failed to decrypt HiveServer2 password for cluster " + env);
                                log.error("Failed to decrypt HiveServer2 password for cluster {}: {}", env, e.getMessage());
                            }
                        }
                    }
                    
                    // Check Metastore Direct password
                    if (cluster != null && cluster.getMetastoreDirect() != null && 
                        cluster.getMetastoreDirect().getConnectionProperties() != null) {
                        String encryptedPwd = cluster.getMetastoreDirect().getConnectionProperties().getProperty("password");
                        if (encryptedPwd != null && !encryptedPwd.isEmpty()) {
                            try {
                                passwordService.decryptPassword(checkRequest.getPasswordKey(), encryptedPwd);
                                log.debug("Successfully decrypted Metastore Direct password for cluster {}", env);
                            } catch (EncryptionException e) {
                                allDecryptedSuccessfully = false;
                                errors.add("Failed to decrypt Metastore Direct password for cluster " + env);
                                log.error("Failed to decrypt Metastore Direct password for cluster {}: {}", env, e.getMessage());
                            } catch (Exception e) {
                                allDecryptedSuccessfully = false;
                                errors.add("Failed to decrypt Metastore Direct password for cluster " + env);
                                log.error("Failed to decrypt Metastore Direct password for cluster {}: {}", env, e.getMessage());
                            }
                        }
                    }
                }
            }
            
            response.put("valid", allDecryptedSuccessfully);
            if (allDecryptedSuccessfully) {
                response.put("message", "Password key is valid. All passwords can be decrypted successfully.");
            } else {
                response.put("message", "Password key is invalid. Unable to decrypt some or all passwords.");
                response.put("errors", errors);
            }
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            log.error("Error checking password key: {}", e.getMessage(), e);
            response.put("valid", false);
            response.put("message", "An error occurred while checking the password key.");
            return ResponseEntity.internalServerError().body(response);
        }
    }
    
    /**
     * Resets encryption by removing all encrypted passwords and marking configuration as not encrypted.
     * @param resetRequest The request containing the configuration to reset.
     * @return A ResponseEntity containing the reset configuration.
     */
    @PostMapping("/reset-encryption")
    @Operation(summary = "Reset encryption",
            description = "Removes all encrypted passwords and marks the configuration as not encrypted. Used when password key is lost.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully reset encryption"),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<HmsMirrorConfig> resetEncryption(@RequestBody ResetEncryptionRequest resetRequest) {
        log.info("Received request to reset encryption.");
        
        try {
            HmsMirrorConfig config = resetRequest.getConfig();
            if (config == null) {
                log.error("Configuration is required for reset.");
                return ResponseEntity.badRequest().body(null);
            }
            
            // Remove all passwords from the configuration
            if (config.getClusters() != null) {
                config.getClusters().forEach((env, cluster) -> {
                    if (cluster != null) {
                        // Clear HiveServer2 passwords
                        if (cluster.getHiveServer2() != null && 
                            cluster.getHiveServer2().getConnectionProperties() != null) {
                            cluster.getHiveServer2().getConnectionProperties().remove("password");
                            log.info("Removed HiveServer2 password for cluster {}", env);
                        }
                        
                        // Clear Metastore Direct passwords
                        if (cluster.getMetastoreDirect() != null && 
                            cluster.getMetastoreDirect().getConnectionProperties() != null) {
                            cluster.getMetastoreDirect().getConnectionProperties().remove("password");
                            log.info("Removed Metastore Direct password for cluster {}", env);
                        }
                    }
                });
            }
            
            // Mark configuration as not encrypted and clear password key
            config.setEncryptedPasswords(false);
            config.setPasswordKey(null);
            
            log.info("Successfully reset encryption. All encrypted passwords have been removed.");
            return ResponseEntity.ok(config);
            
        } catch (Exception e) {
            log.error("Error resetting encryption: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(null);
        }
    }

    /**
     * Decrypts all passwords in the configuration using the provided password key.
     * @param decryptRequest The request containing the configuration and password key.
     * @return A ResponseEntity containing the configuration with decrypted passwords.
     */
    @PostMapping("/decrypt-passwords")
    @Operation(summary = "Decrypt passwords in configuration",
            description = "Decrypts all passwords in the HmsMirror configuration using the provided password key.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully decrypted passwords"),
            @ApiResponse(responseCode = "400", description = "Invalid request or decryption error"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<HmsMirrorConfig> decryptPasswords(@RequestBody DecryptPasswordsRequest decryptRequest) {
        log.info("Received request to decrypt passwords in configuration.");
        
        try {
            if (decryptRequest.getPasswordKey() == null || decryptRequest.getPasswordKey().isEmpty()) {
                log.error("Password key is required for decryption.");
                return ResponseEntity.badRequest().body(null);
            }
            
            HmsMirrorConfig config = decryptRequest.getConfig();
            if (config == null) {
                log.error("Configuration is required for decryption.");
                return ResponseEntity.badRequest().body(null);
            }
            
            // Decrypt passwords in HiveServer2 configurations
            if (config.getClusters() != null) {
                config.getClusters().forEach((env, cluster) -> {
                    if (cluster != null && cluster.getHiveServer2() != null && 
                        cluster.getHiveServer2().getConnectionProperties() != null) {
                        
                        // Decrypt password in connection properties
                        String encryptedPwd = cluster.getHiveServer2().getConnectionProperties().getProperty("password");
                        if (encryptedPwd != null && !encryptedPwd.isEmpty()) {
                            try {
                                String decryptedPwd = passwordService.decryptPassword(decryptRequest.getPasswordKey(), encryptedPwd);
                                cluster.getHiveServer2().getConnectionProperties().setProperty("password", decryptedPwd);
                            } catch (EncryptionException e) {
                                log.error("Failed to decrypt HiveServer2 password for cluster {}: {}", env, e.getMessage());
                                throw new RuntimeException("Failed to decrypt passwords", e);
                            }
                        }
                    }
                    
                    // Decrypt passwords in Metastore Direct configurations
                    if (cluster != null && cluster.getMetastoreDirect() != null && 
                        cluster.getMetastoreDirect().getConnectionProperties() != null) {
                        
                        String encryptedPwd = cluster.getMetastoreDirect().getConnectionProperties().getProperty("password");
                        if (encryptedPwd != null && !encryptedPwd.isEmpty()) {
                            try {
                                String decryptedPwd = passwordService.decryptPassword(decryptRequest.getPasswordKey(), encryptedPwd);
                                cluster.getMetastoreDirect().getConnectionProperties().setProperty("password", decryptedPwd);
                            } catch (EncryptionException e) {
                                log.error("Failed to decrypt Metastore Direct password for cluster {}: {}", env, e.getMessage());
                                throw new RuntimeException("Failed to decrypt passwords", e);
                            }
                        }
                    }
                });
            }
            
            // Mark configuration as not having encrypted passwords
            config.setEncryptedPasswords(false);
            config.setPasswordKey(decryptRequest.getPasswordKey());
            
            log.info("Successfully decrypted passwords in configuration.");
            return ResponseEntity.ok(config);
            
        } catch (Exception e) {
            log.error("Error decrypting passwords: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(null);
        }
    }

    /**
     * Downloads a specific configuration file.
     * @param filename The name of the configuration file to download
     * @return A ResponseEntity containing the file resource
     */
    @GetMapping("/file/{filename}")
    @Operation(summary = "Download configuration file",
            description = "Downloads a specific configuration file from the server.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully downloaded file"),
            @ApiResponse(responseCode = "404", description = "File not found"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<Resource> downloadConfigFile(@PathVariable("filename") String filename) {
        log.info("Received request to download configuration file: {}", filename);
        log.info("Config path: {}", configPath);
        
        try {
            // Check if configPath is properly set
            if (configPath == null || configPath.isEmpty()) {
                log.error("Config path is not properly configured");
                return ResponseEntity.internalServerError().build();
            }
            
            File configDir = new File(configPath);
            if (!configDir.exists() || !configDir.isDirectory()) {
                log.error("Config directory does not exist or is not a directory: {}", configPath);
                return ResponseEntity.internalServerError().build();
            }
            
            File file = new File(configDir, filename);
            log.info("Looking for file at: {}", file.getAbsolutePath());
            
            if (!file.exists() || !file.isFile()) {
                log.error("Configuration file not found at: {}", file.getAbsolutePath());
                return ResponseEntity.notFound().build();
            }
            
            Resource resource = new FileSystemResource(file);
            
            return ResponseEntity.ok()
                    .contentType(MediaType.parseMediaType("application/x-yaml"))
                    .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + filename + "\"")
                    .body(resource);
                    
        } catch (Exception e) {
            log.error("Error downloading configuration file: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Uploads and loads a configuration file.
     * @param file The configuration file to upload
     * @return A ResponseEntity containing the loaded configuration
     */
    @PostMapping("/upload")
    @Operation(summary = "Upload and load configuration file",
            description = "Uploads a configuration file and loads it as the current configuration.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully uploaded and loaded configuration"),
            @ApiResponse(responseCode = "400", description = "Invalid file or configuration"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<HmsMirrorConfig> uploadConfigFile(
            @RequestParam("file") MultipartFile file,
            @RequestParam(value = "filename", required = false) String filename) {
        log.info("=== UPLOAD CONFIG CALLED ===");
        log.info("File original name: {}", file.getOriginalFilename());
        log.info("Filename param: {}", filename);
        
        try {
            if (file.isEmpty()) {
                log.error("Uploaded file is empty");
                return ResponseEntity.badRequest().build();
            }
            
            // Use the filename parameter if provided, otherwise use the original filename
            String actualFilename = (filename != null && !filename.isEmpty()) ? filename : file.getOriginalFilename();
            if (actualFilename == null || (!actualFilename.endsWith(".yaml") && !actualFilename.endsWith(".yml"))) {
                log.error("Invalid file type: {}", actualFilename);
                return ResponseEntity.badRequest().build();
            }
            log.info("Using filename: {}", actualFilename);
            
            // Save the uploaded file temporarily
            Path tempFile = Files.createTempFile("upload-", actualFilename);
            file.transferTo(tempFile.toFile());
            
            try {
                // Load and validate the configuration
                HmsMirrorConfig uploadedConfig = domainService.deserializeConfig(tempFile.toString());
                
                if (uploadedConfig == null) {
                    log.error("Failed to parse uploaded configuration");
                    return ResponseEntity.badRequest().build();
                }
                
                // Close any existing session first
                try {
                    executeSessionService.closeSession();
                } catch (SessionException e) {
                    log.warn("Error closing existing session: {}", e.getMessage());
                }
                
                // Log the uploaded config details
                log.info("Uploaded config has clusters? {}", uploadedConfig.getClusters() != null);
                if (uploadedConfig.getClusters() != null) {
                    log.info("Uploaded config cluster count: {}", uploadedConfig.getClusters().size());
                    log.info("Uploaded config clusters: {}", uploadedConfig.getClusters().keySet());
                }
                log.info("Uploaded config has databases? {}", uploadedConfig.getDatabases() != null);
                if (uploadedConfig.getDatabases() != null) {
                    log.info("Uploaded config database count: {}", uploadedConfig.getDatabases().size());
                }
                
                // Create and set a new ExecuteSession as the single source of truth
                // Don't bother with ConfigurationService - just use ExecuteSessionService
                String sessionId = actualFilename != null ? actualFilename.replace(".yaml", "").replace(".yml", "") : "uploaded-config";
                log.info("Creating new session with ID: {}", sessionId);
                com.cloudera.utils.hms.mirror.domain.support.ExecuteSession session = executeSessionService.createSession(sessionId, uploadedConfig);
                log.info("Created session, setting as current...");
                executeSessionService.setSession(session);
                
                // Also update ConfigurationService to keep it in sync (for any legacy code that might use it)
                configService.replaceConfiguration(uploadedConfig);
                
                // Verify it was set
                com.cloudera.utils.hms.mirror.domain.support.ExecuteSession verifySession = executeSessionService.getSession();
                log.info("Verification - Session is null? {}", verifySession == null);
                if (verifySession != null) {
                    log.info("Verification - Session ID: {}", verifySession.getSessionId());
                    log.info("Verification - Config is null? {}", verifySession.getConfig() == null);
                }
                
                log.info("Successfully uploaded and loaded configuration from file: {}", actualFilename);
                return ResponseEntity.ok(uploadedConfig);
                
            } finally {
                // Clean up temp file
                Files.deleteIfExists(tempFile);
            }
            
        } catch (Exception e) {
            log.error("Error uploading configuration file: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Converts a configuration to YAML format.
     * @param config The configuration to convert to YAML
     * @return A ResponseEntity containing the YAML representation
     */
    @PostMapping("/to-yaml")
    @Operation(summary = "Convert configuration to YAML",
            description = "Converts an HmsMirrorConfig object to YAML format for display.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully converted to YAML"),
            @ApiResponse(responseCode = "400", description = "Invalid configuration"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<Map<String, String>> convertToYaml(@RequestBody HmsMirrorConfig config) {
        log.info("Received request to convert configuration to YAML.");
        
        try {
            if (config == null) {
                log.error("Configuration is null");
                return ResponseEntity.badRequest().body(Map.of("error", "Configuration is required"));
            }
            
            // Convert configuration to YAML using ObjectMapper
            ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
            yamlMapper.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, false);
            yamlMapper.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
            
            String yamlContent = yamlMapper.writeValueAsString(config);
            
            log.info("Successfully converted configuration to YAML.");
            return ResponseEntity.ok(Map.of("yaml", yamlContent));
            
        } catch (Exception e) {
            log.error("Error converting configuration to YAML: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(Map.of("error", "Failed to convert to YAML"));
        }
    }

    /**
     * Request class for encrypting passwords.
     */
    public static class EncryptPasswordsRequest {
        private HmsMirrorConfig config;
        private String passwordKey;

        public HmsMirrorConfig getConfig() {
            return config;
        }

        public void setConfig(HmsMirrorConfig config) {
            this.config = config;
        }

        public String getPasswordKey() {
            return passwordKey;
        }

        public void setPasswordKey(String passwordKey) {
            this.passwordKey = passwordKey;
        }
    }

    /**
     * Request class for decrypting passwords.
     */
    public static class DecryptPasswordsRequest {
        private HmsMirrorConfig config;
        private String passwordKey;

        public HmsMirrorConfig getConfig() {
            return config;
        }

        public void setConfig(HmsMirrorConfig config) {
            this.config = config;
        }

        public String getPasswordKey() {
            return passwordKey;
        }

        public void setPasswordKey(String passwordKey) {
            this.passwordKey = passwordKey;
        }
    }

    /**
     * Request class for checking password key validity.
     */
    public static class CheckPasswordKeyRequest {
        private HmsMirrorConfig config;
        private String passwordKey;

        public HmsMirrorConfig getConfig() {
            return config;
        }

        public void setConfig(HmsMirrorConfig config) {
            this.config = config;
        }

        public String getPasswordKey() {
            return passwordKey;
        }

        public void setPasswordKey(String passwordKey) {
            this.passwordKey = passwordKey;
        }
    }

    /**
     * Request class for resetting encryption.
     */
    public static class ResetEncryptionRequest {
        private HmsMirrorConfig config;

        public HmsMirrorConfig getConfig() {
            return config;
        }

        public void setConfig(HmsMirrorConfig config) {
            this.config = config;
        }
    }

    /**
     * Metadata class for configuration files.
     */
    public static class ConfigFileMetadata {
        private String filename;
        private String dataStrategy;
        private long lastModified;
        private long size;

        public ConfigFileMetadata(String filename, String dataStrategy, long lastModified, long size) {
            this.filename = filename;
            this.dataStrategy = dataStrategy;
            this.lastModified = lastModified;
            this.size = size;
        }

        public String getFilename() {
            return filename;
        }

        public void setFilename(String filename) {
            this.filename = filename;
        }

        public String getDataStrategy() {
            return dataStrategy;
        }

        public void setDataStrategy(String dataStrategy) {
            this.dataStrategy = dataStrategy;
        }

        public long getLastModified() {
            return lastModified;
        }

        public void setLastModified(long lastModified) {
            this.lastModified = lastModified;
        }

        public long getSize() {
            return size;
        }

        public void setSize(long size) {
            this.size = size;
        }
    }
}
