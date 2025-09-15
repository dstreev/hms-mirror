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

import com.cloudera.utils.hive.config.ConnectionPool;
import com.cloudera.utils.hive.config.DBStore;
import com.cloudera.utils.hms.mirror.domain.Cluster; // Import domain object
import com.cloudera.utils.hms.mirror.domain.HiveServer2Config; // Import domain object
import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig; // Import top-level domain object
import com.cloudera.utils.hms.mirror.domain.support.Environment; // Import Environment enum
import com.cloudera.utils.hms.mirror.web.controller.api.v2.service.ConfigurationService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.Properties; // For connection properties
import java.util.TreeMap; // For map creation

/**
 * REST Controller for managing the 'clusters' section of the HmsMirror configuration.
 * This includes managing properties for both LEFT and RIGHT clusters.
 * Now directly works with Map<Environment, Cluster> from the domain.
 */
@RestController("ClustersControllerV2")
@RequestMapping("/api/v2/config/clusters")
@Tag(name = "Clusters Configuration", description = "Operations related to the LEFT and RIGHT cluster configurations.")
@Slf4j
public class ClustersController {

    @Autowired
    private ConfigurationService configService;

    /**
     * Retrieves the current clusters configuration (both LEFT and RIGHT).
     * @return A ResponseEntity containing the Map<Environment, Cluster>.
     */
    @GetMapping(produces = "application/json")
    @Operation(summary = "Get current clusters configuration",
            description = "Retrieves the complete current configuration for both LEFT and RIGHT clusters.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully retrieved clusters configuration"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<Map<Environment, Cluster>> getClustersConfig() {
        log.info("Received request to get clusters configuration.");
        Map<Environment, Cluster> clusters = configService.getConfiguration().getClusters();
        log.info("Returning clusters configuration: {}", clusters);
        return ResponseEntity.ok(clusters);
    }

    /**
     * Updates the clusters configuration (both LEFT and RIGHT).
     * Only provided (non-null) fields will update the existing configuration.
     * @param clusters A partial Map<Environment, Cluster> with fields to update.
     * @return A ResponseEntity containing the updated Map<Environment, Cluster>.
     */
    @PutMapping(produces = "application/json", consumes = "application/json")
    @Operation(summary = "Update clusters configuration",
            description = "Updates specific properties within the clusters configuration (both LEFT and RIGHT). " +
                    "Only provided (non-null) fields will be updated.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully updated clusters configuration"),
            @ApiResponse(responseCode = "400", description = "Invalid request body"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<Map<Environment, Cluster>> updateClustersConfig(@RequestBody Map<Environment, Cluster> clusters) {
        log.info("Received request to update clusters configuration with data: {}", clusters);
        HmsMirrorConfig currentHmsMirrorConfig = configService.getConfiguration();
        // The init() method ensures clusters map is never null.
        if (currentHmsMirrorConfig.getClusters() == null) {
            currentHmsMirrorConfig.setClusters(new TreeMap<>());
            log.debug("Clusters map was null, initialized a new one.");
        }
        configService.updateClusters(currentHmsMirrorConfig.getClusters(), clusters);
        log.info("Clusters configuration updated successfully.");
        return ResponseEntity.ok(currentHmsMirrorConfig.getClusters());
    }

    /**
     * Retrieves the current LEFT cluster configuration.
     * @return A ResponseEntity containing the Cluster domain object.
     */
    @GetMapping("/left")
    @Operation(summary = "Get current LEFT cluster configuration",
            description = "Retrieves the complete current configuration for the LEFT cluster.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully retrieved LEFT cluster configuration"),
            @ApiResponse(responseCode = "404", description = "LEFT cluster not found"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<Cluster> getLeftClusterConfig() {
        log.info("Received request to get LEFT cluster configuration.");
        Cluster leftCluster = configService.getCluster(Environment.LEFT);
        if (leftCluster == null) {
            log.warn("LEFT cluster configuration not found.");
            return ResponseEntity.notFound().build();
        }
        log.info("Returning LEFT cluster configuration: {}", leftCluster);
        return ResponseEntity.ok(leftCluster);
    }

    /**
     * Updates the LEFT cluster configuration.
     * Only provided (non-null) fields will update the existing configuration.
     * @param cluster A partial Cluster domain object with fields to update for the LEFT cluster.
     * @return A ResponseEntity containing the updated LEFT Cluster domain object.
     */
    @PutMapping("/left")
    @Operation(summary = "Update LEFT cluster configuration",
            description = "Updates specific properties within the LEFT cluster configuration. " +
                    "Only provided (non-null) fields will be updated.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully updated LEFT cluster configuration"),
            @ApiResponse(responseCode = "400", description = "Invalid request body"),
            @ApiResponse(responseCode = "404", description = "LEFT cluster not found"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<Cluster> updateLeftClusterConfig(@RequestBody Cluster cluster) {
        log.info("Received request to update LEFT cluster configuration with data: {}", cluster);
        // The service method handles creation if the cluster doesn't exist.
        configService.updateSpecificCluster(Environment.LEFT, cluster);
        log.info("LEFT cluster configuration updated successfully.");
        return ResponseEntity.ok(configService.getCluster(Environment.LEFT)); // Retrieve updated state
    }

    /**
     * Retrieves the current RIGHT cluster configuration.
     * @return A ResponseEntity containing the Cluster domain object.
     */
    @GetMapping("/right")
    @Operation(summary = "Get current RIGHT cluster configuration",
            description = "Retrieves the complete current configuration for the RIGHT cluster.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully retrieved RIGHT cluster configuration"),
            @ApiResponse(responseCode = "404", description = "RIGHT cluster not found"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<Cluster> getRightClusterConfig() {
        log.info("Received request to get RIGHT cluster configuration.");
        Cluster rightCluster = configService.getCluster(Environment.RIGHT);
        if (rightCluster == null) {
            log.warn("RIGHT cluster configuration not found.");
            return ResponseEntity.notFound().build();
        }
        log.info("Returning RIGHT cluster configuration: {}", rightCluster);
        return ResponseEntity.ok(rightCluster);
    }

    /**
     * Updates the RIGHT cluster configuration.
     * Only provided (non-null) fields will update the existing configuration.
     * @param cluster A partial Cluster domain object with fields to update for the RIGHT cluster.
     * @return A ResponseEntity containing the updated RIGHT Cluster domain object.
     */
    @PutMapping("/right")
    @Operation(summary = "Update RIGHT cluster configuration",
            description = "Updates specific properties within the RIGHT cluster configuration. " +
                    "Only provided (non-null) fields will be updated.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully updated RIGHT cluster configuration"),
            @ApiResponse(responseCode = "400", description = "Invalid request body"),
            @ApiResponse(responseCode = "404", description = "RIGHT cluster not found"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<Cluster> updateRightClusterConfig(@RequestBody Cluster cluster) {
        log.info("Received request to update RIGHT cluster configuration with data: {}", cluster);
        // The service method handles creation if the cluster doesn't exist.
        configService.updateSpecificCluster(Environment.RIGHT, cluster);
        log.info("RIGHT cluster configuration updated successfully.");
        return ResponseEntity.ok(configService.getCluster(Environment.RIGHT)); // Retrieve updated state
    }


    /*
     * HiveServer2 Endpoints for LEFT/RIGHT clusters
     */

    @GetMapping("/{clusterName}/hiveServer2")
    @Operation(summary = "Get HiveServer2 configuration for a specific cluster",
            description = "Retrieves the HiveServer2 configuration for the specified LEFT or RIGHT cluster.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully retrieved HiveServer2 configuration"),
            @ApiResponse(responseCode = "404", description = "Cluster not found or HiveServer2 configuration is null"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<HiveServer2Config> getHiveServer2Config(@PathVariable String clusterName) {
        log.info("Received request to get HiveServer2 config for cluster: {}", clusterName);
        Cluster cluster = configService.getCluster(Environment.valueOf(clusterName.toUpperCase()));
        if (cluster != null && cluster.getHiveServer2() != null) {
            log.info("Returning HiveServer2 config for cluster {}: {}", clusterName, cluster.getHiveServer2());
            return ResponseEntity.ok(cluster.getHiveServer2());
        } else {
            log.warn("HiveServer2 config not found for cluster: {}", clusterName);
            return ResponseEntity.notFound().build();
        }
    }

    @PutMapping("/{clusterName}/hiveServer2")
    @Operation(summary = "Update HiveServer2 configuration for a specific cluster",
            description = "Updates specific properties within the HiveServer2 configuration for the specified LEFT or RIGHT cluster. " +
                    "Only provided (non-null) fields will be updated.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully updated HiveServer2 configuration"),
            @ApiResponse(responseCode = "400", description = "Invalid request body"),
            @ApiResponse(responseCode = "404", description = "Cluster not found"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<HiveServer2Config> updateHiveServer2Config(@PathVariable String clusterName, @RequestBody HiveServer2Config hiveServer2Config) {
        log.info("Received request to update HiveServer2 config for cluster {}: {}", clusterName, hiveServer2Config);
        Cluster cluster = configService.getCluster(Environment.valueOf(clusterName.toUpperCase()));
        if (cluster != null) {
            if (cluster.getHiveServer2() == null) {
                cluster.setHiveServer2(new HiveServer2Config());
                log.debug("HiveServer2Config object was null for cluster {}, initialized a new one.", clusterName);
            }
            configService.updateHiveServer2Config(cluster.getHiveServer2(), hiveServer2Config);
            log.info("HiveServer2 configuration for cluster {} updated successfully.", clusterName);
            return ResponseEntity.ok(cluster.getHiveServer2());
        } else {
            log.warn("Cluster '{}' not found for HiveServer2 update.", clusterName);
            return ResponseEntity.notFound().build();
        }
    }

    /*
     * MetastoreDirect Endpoints for LEFT/RIGHT clusters
     */

    @GetMapping("/{clusterName}/metastoreDirect")
    @Operation(summary = "Get MetastoreDirect configuration for a specific cluster",
            description = "Retrieves the MetastoreDirect configuration for the specified LEFT or RIGHT cluster.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully retrieved MetastoreDirect configuration"),
            @ApiResponse(responseCode = "404", description = "Cluster not found or MetastoreDirect configuration is null"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<DBStore> getMetastoreDirectConfig(@PathVariable String clusterName) {
        log.info("Received request to get MetastoreDirect config for cluster: {}", clusterName);
        Cluster cluster = configService.getCluster(Environment.valueOf(clusterName.toUpperCase()));
        if (cluster != null && cluster.getMetastoreDirect() != null) {
            log.info("Returning MetastoreDirect config for cluster {}: {}", clusterName, cluster.getMetastoreDirect());
            return ResponseEntity.ok(cluster.getMetastoreDirect());
        } else {
            log.warn("MetastoreDirect config not found for cluster: {}", clusterName);
            return ResponseEntity.notFound().build();
        }
    }

    @PutMapping("/{clusterName}/metastoreDirect")
    @Operation(summary = "Update MetastoreDirect configuration for a specific cluster",
            description = "Updates specific properties within the MetastoreDirect configuration for the specified LEFT or RIGHT cluster. " +
                    "Only provided (non-null) fields will be updated.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully updated MetastoreDirect configuration"),
            @ApiResponse(responseCode = "400", description = "Invalid request body"),
            @ApiResponse(responseCode = "404", description = "Cluster not found"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<DBStore> updateMetastoreDirectConfig(@PathVariable String clusterName, @RequestBody DBStore metastoreDirect) {
        log.info("Received request to update MetastoreDirect config for cluster {}: {}", clusterName, metastoreDirect);
        Cluster cluster = configService.getCluster(Environment.valueOf(clusterName.toUpperCase()));
        if (cluster != null) {
            if (cluster.getMetastoreDirect() == null) {
                cluster.setMetastoreDirect(new DBStore());
                log.debug("MetastoreDirect object was null for cluster {}, initialized a new one.", clusterName);
            }
            configService.updateMetastoreDirect(cluster.getMetastoreDirect(), metastoreDirect);
            log.info("MetastoreDirect configuration for cluster {} updated successfully.", clusterName);
            return ResponseEntity.ok(cluster.getMetastoreDirect());
        } else {
            log.warn("Cluster '{}' not found for MetastoreDirect update.", clusterName);
            return ResponseEntity.notFound().build();
        }
    }

    /*
     * MetastoreDirect Connection Pool Endpoints for LEFT/RIGHT clusters
     */

    @GetMapping("/{clusterName}/metastoreDirect/connectionPool")
    @Operation(summary = "Get MetastoreDirect connection pool configuration for a specific cluster",
            description = "Retrieves the connection pool configuration for MetastoreDirect of the specified LEFT or RIGHT cluster.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully retrieved connection pool configuration"),
            @ApiResponse(responseCode = "404", description = "Cluster or MetastoreDirect/ConnectionPool not found"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<ConnectionPool> getMetastoreDirectConnectionPoolConfig(@PathVariable String clusterName) {
        log.info("Received request to get MetastoreDirect connection pool config for cluster: {}", clusterName);
        Cluster cluster = configService.getCluster(Environment.valueOf(clusterName.toUpperCase()));
        if (cluster != null && cluster.getMetastoreDirect() != null && cluster.getMetastoreDirect().getConnectionPool() != null) {
            log.info("Returning MetastoreDirect connection pool config for cluster {}: {}", clusterName, cluster.getMetastoreDirect().getConnectionPool());
            return ResponseEntity.ok(cluster.getMetastoreDirect().getConnectionPool());
        } else {
            log.warn("MetastoreDirect connection pool config not found for cluster: {}", clusterName);
            return ResponseEntity.notFound().build();
        }
    }

    @PutMapping("/{clusterName}/metastoreDirect/connectionPool")
    @Operation(summary = "Update MetastoreDirect connection pool configuration for a specific cluster",
            description = "Updates specific properties within the MetastoreDirect connection pool configuration for the specified LEFT or RIGHT cluster. " +
                    "Only provided (non-null) fields will be updated.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully updated connection pool configuration"),
            @ApiResponse(responseCode = "400", description = "Invalid request body"),
            @ApiResponse(responseCode = "404", description = "Cluster or MetastoreDirect not found"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<ConnectionPool> updateMetastoreDirectConnectionPoolConfig(@PathVariable String clusterName, @RequestBody ConnectionPool connectionPool) {
        log.info("Received request to update MetastoreDirect connection pool config for cluster {}: {}", clusterName, connectionPool);
        Cluster cluster = configService.getCluster(Environment.valueOf(clusterName.toUpperCase()));
        if (cluster != null && cluster.getMetastoreDirect() != null) {
            if (cluster.getMetastoreDirect().getConnectionPool() == null) {
                cluster.getMetastoreDirect().setConnectionPool(new ConnectionPool());
                log.debug("ConnectionPool object was null for metastoreDirect, initialized a new one.");
            }
            configService.updateConnectionPool(cluster.getMetastoreDirect().getConnectionPool(), connectionPool);
            log.info("MetastoreDirect connection pool configuration for cluster {} updated successfully.", clusterName);
            return ResponseEntity.ok(cluster.getMetastoreDirect().getConnectionPool());
        } else {
            log.warn("Cluster '{}' or its MetastoreDirect configuration not found for connection pool update.", clusterName);
            return ResponseEntity.notFound().build();
        }
    }


    /*
     * HiveServer2 Connection Properties Endpoints
     */

    @GetMapping("/{clusterName}/hiveServer2/connectionProperties")
    @Operation(summary = "Get HiveServer2 connection properties for a specific cluster",
            description = "Retrieves the connection properties map for HiveServer2 of the specified cluster.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully retrieved connection properties map"),
            @ApiResponse(responseCode = "404", description = "Cluster or HiveServer2/ConnectionProperties not found"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<Properties> getHiveServer2ConnectionProperties(@PathVariable String clusterName) {
        log.info("Received request to get HiveServer2 connection properties for cluster: {}", clusterName);
        Properties properties = configService.getHiveServer2ConnectionProperties(clusterName);
        // Additional check for existence of the parent object, even if properties map is empty
        Cluster cluster = configService.getCluster(Environment.valueOf(clusterName.toUpperCase()));
        if (cluster != null && cluster.getHiveServer2() != null) {
            log.info("Returning HiveServer2 connection properties for cluster {}: {} entries.", clusterName, properties.size());
            return ResponseEntity.ok(properties);
        } else {
            log.warn("HiveServer2 connection properties not found for cluster: {}", clusterName);
            return ResponseEntity.notFound().build();
        }
    }

    @PostMapping("/{clusterName}/hiveServer2/connectionProperties/{key}")
    @Operation(summary = "Add or update a HiveServer2 connection property for a specific cluster",
            description = "Adds a new or updates an existing connection property for HiveServer2 of the specified cluster.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully added/updated property"),
            @ApiResponse(responseCode = "400", description = "Invalid request body or key"),
            @ApiResponse(responseCode = "404", description = "Cluster or HiveServer2 not found"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<String> addOrUpdateHiveServer2ConnectionProperty(@PathVariable String clusterName, @PathVariable String key, @RequestBody String value) {
        log.info("Received request to add/update HiveServer2 connection property for cluster '{}': key='{}', value='{}'", clusterName, key, value);
        Cluster cluster = configService.getCluster(Environment.valueOf(clusterName.toUpperCase()));
        if (cluster != null && cluster.getHiveServer2() != null) {
            if (cluster.getHiveServer2().getConnectionProperties() == null) {
                cluster.getHiveServer2().setConnectionProperties(new Properties());
            }
            configService.addHiveServer2ConnectionProperty(clusterName, key, value);
            log.info("HiveServer2 connection property for cluster '{}', key='{}' set to value='{}'.", clusterName, key, value);
            return ResponseEntity.ok("Property '" + key + "' set to '" + value + "' for HiveServer2 of cluster '" + clusterName + "'.");
        } else {
            log.warn("Cluster '{}' or its HiveServer2 configuration not found for connection property update.", clusterName);
            return ResponseEntity.notFound().build();
        }
    }

    @DeleteMapping("/{clusterName}/hiveServer2/connectionProperties/{key}")
    @Operation(summary = "Delete a HiveServer2 connection property for a specific cluster",
            description = "Deletes a connection property from HiveServer2 of the specified cluster by its key.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully deleted property or property not found"),
            @ApiResponse(responseCode = "404", description = "Cluster or HiveServer2 not found"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<String> deleteHiveServer2ConnectionProperty(@PathVariable String clusterName, @PathVariable String key) {
        log.info("Received request to delete HiveServer2 connection property for cluster '{}', key: '{}'", clusterName, key);
        Cluster cluster = configService.getCluster(Environment.valueOf(clusterName.toUpperCase()));
        if (cluster != null && cluster.getHiveServer2() != null && cluster.getHiveServer2().getConnectionProperties() != null) {
            if (cluster.getHiveServer2().getConnectionProperties().containsKey(key)) {
                configService.deleteHiveServer2ConnectionProperty(clusterName, key);
                log.info("HiveServer2 connection property for cluster '{}', key='{}' deleted.", clusterName, key);
                return ResponseEntity.ok("Property '" + key + "' deleted from HiveServer2 of cluster '" + clusterName + "'.");
            } else {
                log.warn("HiveServer2 connection property for cluster '{}', key='{}' not found. No deletion performed.", clusterName, key);
                return ResponseEntity.ok("Property '" + key + "' not found for HiveServer2 of cluster '" + clusterName + "'.");
            }
        } else {
            log.warn("Cluster '{}' or its HiveServer2 configuration not found for deletion.", clusterName);
            return ResponseEntity.notFound().build();
        }
    }

    @DeleteMapping("/{clusterName}/hiveServer2/connectionProperties/clear")
    @Operation(summary = "Clear all HiveServer2 connection properties for a specific cluster",
            description = "Removes all connection properties from HiveServer2 of the specified cluster.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Successfully cleared properties"),
            @ApiResponse(responseCode = "404", description = "Cluster or HiveServer2 not found"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<Void> clearHiveServer2ConnectionProperties(@PathVariable String clusterName) {
        log.info("Received request to clear HiveServer2 connection properties for cluster: {}", clusterName);
        Cluster cluster = configService.getCluster(Environment.valueOf(clusterName.toUpperCase()));
        if (cluster != null && cluster.getHiveServer2() != null && cluster.getHiveServer2().getConnectionProperties() != null) {
            configService.clearHiveServer2ConnectionProperties(clusterName);
            log.info("HiveServer2 connection properties for cluster '{}' cleared successfully.", clusterName);
            return ResponseEntity.noContent().build();
        } else {
            log.warn("Cluster '{}' or its HiveServer2 configuration not found for clearing connection properties.", clusterName);
            return ResponseEntity.notFound().build();
        }
    }

    /*
     * MetastoreDirect Connection Properties Endpoints
     */

    @GetMapping("/{clusterName}/metastoreDirect/connectionProperties")
    @Operation(summary = "Get MetastoreDirect connection properties for a specific cluster",
            description = "Retrieves the connection properties map for MetastoreDirect of the specified cluster.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully retrieved connection properties map"),
            @ApiResponse(responseCode = "404", description = "Cluster or MetastoreDirect/ConnectionProperties not found"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<Properties> getMetastoreDirectConnectionProperties(@PathVariable String clusterName) {
        log.info("Received request to get MetastoreDirect connection properties for cluster: {}", clusterName);
        Properties properties = configService.getMetastoreDirectConnectionProperties(clusterName);
        Cluster cluster = configService.getCluster(Environment.valueOf(clusterName.toUpperCase()));
        if (cluster != null && cluster.getMetastoreDirect() != null) {
            log.info("Returning MetastoreDirect connection properties for cluster {}: {} entries.", clusterName, properties.size());
            return ResponseEntity.ok(properties);
        } else {
            log.warn("MetastoreDirect connection properties not found for cluster: {}", clusterName);
            return ResponseEntity.notFound().build();
        }
    }

    @PostMapping("/{clusterName}/metastoreDirect/connectionProperties/{key}")
    @Operation(summary = "Add or update a MetastoreDirect connection property for a specific cluster",
            description = "Adds a new or updates an existing connection property for MetastoreDirect of the specified cluster.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully added/updated property"),
            @ApiResponse(responseCode = "400", description = "Invalid request body or key"),
            @ApiResponse(responseCode = "404", description = "Cluster or MetastoreDirect not found"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<String> addOrUpdateMetastoreDirectConnectionProperty(@PathVariable String clusterName, @PathVariable String key, @RequestBody String value) {
        log.info("Received request to add/update MetastoreDirect connection property for cluster '{}': key='{}', value='{}'", clusterName, key, value);
        Cluster cluster = configService.getCluster(Environment.valueOf(clusterName.toUpperCase()));
        if (cluster != null && cluster.getMetastoreDirect() != null) {
            if (cluster.getMetastoreDirect().getConnectionProperties() == null) {
                cluster.getMetastoreDirect().setConnectionProperties(new Properties());
            }
            configService.addMetastoreDirectConnectionProperty(clusterName, key, value);
            log.info("MetastoreDirect connection property for cluster '{}', key='{}' set to value='{}'.", clusterName, key, value);
            return ResponseEntity.ok("Property '" + key + "' set to '" + value + "' for MetastoreDirect of cluster '" + clusterName + "'.");
        } else {
            log.warn("Cluster '{}' or its MetastoreDirect configuration not found for connection property update.", clusterName);
            return ResponseEntity.notFound().build();
        }
    }

    @DeleteMapping("/{clusterName}/metastoreDirect/connectionProperties/{key}")
    @Operation(summary = "Delete a MetastoreDirect connection property for a specific cluster",
            description = "Deletes a connection property from MetastoreDirect of the specified cluster by its key.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully deleted property or property not found"),
            @ApiResponse(responseCode = "404", description = "Cluster or MetastoreDirect not found"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<String> deleteMetastoreDirectConnectionProperty(@PathVariable String clusterName, @PathVariable String key) {
        log.info("Received request to delete MetastoreDirect connection property for cluster '{}', key: '{}'", clusterName, key);
        Cluster cluster = configService.getCluster(Environment.valueOf(clusterName.toUpperCase()));
        if (cluster != null && cluster.getMetastoreDirect() != null && cluster.getMetastoreDirect().getConnectionProperties() != null) {
            if (cluster.getMetastoreDirect().getConnectionProperties().containsKey(key)) {
                configService.deleteMetastoreDirectConnectionProperty(clusterName, key);
                log.info("MetastoreDirect connection property for cluster '{}', key='{}' deleted.", clusterName, key);
                return ResponseEntity.ok("Property '" + key + "' deleted from MetastoreDirect of cluster '" + clusterName + "'.");
            } else {
                log.warn("MetastoreDirect connection property for cluster '{}', key='{}' not found. No deletion performed.", clusterName, key);
                return ResponseEntity.ok("Property '" + key + "' not found for MetastoreDirect of cluster '" + clusterName + "'.");
            }
        } else {
            log.warn("Cluster '{}' or its MetastoreDirect configuration not found for deletion.", clusterName);
            return ResponseEntity.notFound().build();
        }
    }

    @DeleteMapping("/{clusterName}/metastoreDirect/connectionProperties/clear")
    @Operation(summary = "Clear all MetastoreDirect connection properties for a specific cluster",
            description = "Removes all connection properties from MetastoreDirect of the specified cluster.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Successfully cleared properties"),
            @ApiResponse(responseCode = "404", description = "Cluster or MetastoreDirect not found"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<Void> clearMetastoreDirectConnectionProperties(@PathVariable String clusterName) {
        log.info("Received request to clear MetastoreDirect connection properties for cluster: {}", clusterName);
        Cluster cluster = configService.getCluster(Environment.valueOf(clusterName.toUpperCase()));
        if (cluster != null && cluster.getMetastoreDirect() != null && cluster.getMetastoreDirect().getConnectionProperties() != null) {
            configService.clearMetastoreDirectConnectionProperties(clusterName);
            log.info("MetastoreDirect connection properties for cluster '{}' cleared successfully.", clusterName);
            return ResponseEntity.noContent().build();
        } else {
            log.warn("Cluster '{}' or its MetastoreDirect configuration not found for clearing connection properties.", clusterName);
            return ResponseEntity.notFound().build();
        }
    }
}
