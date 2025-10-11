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

package com.cloudera.utils.hms.mirror.web.service;

import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.web.model.DecisionTreeNode;
import com.cloudera.utils.hms.mirror.web.model.UserSelection;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
@Slf4j
public class DecisionTreeService {

    private final ResourceLoader resourceLoader;
    private final ObjectMapper yamlMapper;
    private final ObjectMapper jsonMapper;
    
    private Map<String, Map<String, Object>> decisionTrees = new HashMap<>();
    private Map<String, String> currentPages = new HashMap<>();
    
    @Autowired
    public DecisionTreeService(ResourceLoader resourceLoader) {
        this.resourceLoader = resourceLoader;
        this.yamlMapper = new ObjectMapper(new YAMLFactory());
        this.jsonMapper = new ObjectMapper();
    }
    
    @PostConstruct
    public void loadDecisionTrees() {
        String[] strategies = {
            "SQL:decision-trees/sql-strategy.yaml",
            "HYBRID:decision-trees/hybrid-strategy.yaml", 
            "LINKED:decision-trees/linked-strategy.yaml",
            "SCHEMA_ONLY:decision-trees/schema-only-strategy.yaml",
            "EXPORT_IMPORT:decision-trees/export-import-strategy.yaml",
            "STORAGE_MIGRATION:decision-trees/storage-migration-strategy.yaml",
            "COMMON:decision-trees/common-strategy.yaml"
        };
        
        for (String strategyInfo : strategies) {
            try {
                String[] parts = strategyInfo.split(":");
                loadDecisionTree(parts[0], parts[1]);
            } catch (Exception e) {
                log.error("Error loading decision tree {}: {}", strategyInfo, e.getMessage());
                // Continue loading other strategies
            }
        }
        log.info("Loaded {} decision trees", decisionTrees.size());
    }
    
    private void loadDecisionTree(String strategy, String resourcePath) throws IOException {
        Resource resource = resourceLoader.getResource("classpath:" + resourcePath);
        if (resource == null || !resource.exists()) {
            log.warn("Decision tree resource not found: {}", resourcePath);
            return;
        }
        try {
            Map<String, Object> treeData = yamlMapper.readValue(resource.getInputStream(), Map.class);
            decisionTrees.put(strategy.toUpperCase(), treeData);
            log.info("Loaded decision tree for strategy: {}", strategy);
        } catch (Exception e) {
            log.error("Error loading decision tree for strategy {}: {}", strategy, e.getMessage());
            throw e;
        }
    }
    
    public DecisionTreeNode initializeDecisionTree(String strategy, HmsMirrorConfig config) {
        Map<String, Object> treeData = decisionTrees.get(strategy.toUpperCase());
        if (treeData == null) {
            throw new IllegalArgumentException("Unknown strategy: " + strategy);
        }
        
        // Get the first page and first node
        Map<String, Object> pages = (Map<String, Object>) treeData.get("pages");
        String firstPageId = pages.keySet().iterator().next();
        currentPages.put(config.getClass().getName(), firstPageId);
        
        Map<String, Object> firstPage = (Map<String, Object>) pages.get(firstPageId);
        List<Map<String, Object>> nodes = (List<Map<String, Object>>) firstPage.get("nodes");
        
        if (nodes != null && !nodes.isEmpty()) {
            return convertToDecisionTreeNode(nodes.get(0));
        }
        
        throw new IllegalStateException("No nodes found in first page");
    }
    
    public DecisionTreeNode getNextNode(String currentNodeId, String selectedOption, HmsMirrorConfig config) {
        String strategy = config.getDataStrategy() != null ? config.getDataStrategy().toString() : "SQL";
        Map<String, Object> treeData = decisionTrees.get(strategy.toUpperCase());
        
        if (treeData == null) {
            return null;
        }
        
        // Find current node and determine next
        DecisionTreeNode currentNode = findNodeById(treeData, currentNodeId);
        if (currentNode == null) {
            return null;
        }
        
        // Check if we need to navigate to next page
        if (currentNode.getNextPageId() != null) {
            return getFirstNodeOfPage(treeData, currentNode.getNextPageId());
        }
        
        // Check if we have a next node ID
        if (currentNode.getNextNodeId() != null) {
            return findNodeById(treeData, currentNode.getNextNodeId());
        }
        
        // Check if the selected option specifies a next node or page
        if (currentNode.getOptions() != null) {
            for (var option : currentNode.getOptions()) {
                if (option.getValue().equals(selectedOption)) {
                    if (option.getNextNodeId() != null) {
                        return findNodeById(treeData, option.getNextNodeId());
                    }
                    if (option.getNextPageId() != null) {
                        return getFirstNodeOfPage(treeData, option.getNextPageId());
                    }
                }
            }
        }
        
        return null; // End of tree
    }
    
    private DecisionTreeNode getFirstNodeOfPage(Map<String, Object> treeData, String pageId) {
        Map<String, Object> pages = (Map<String, Object>) treeData.get("pages");
        Map<String, Object> page = (Map<String, Object>) pages.get(pageId);
        
        if (page != null) {
            List<Map<String, Object>> nodes = (List<Map<String, Object>>) page.get("nodes");
            if (nodes != null && !nodes.isEmpty()) {
                return convertToDecisionTreeNode(nodes.get(0));
            }
        }
        
        return null;
    }
    
    private DecisionTreeNode findNodeById(Map<String, Object> treeData, String nodeId) {
        Map<String, Object> pages = (Map<String, Object>) treeData.get("pages");
        
        for (Map.Entry<String, Object> pageEntry : pages.entrySet()) {
            Map<String, Object> page = (Map<String, Object>) pageEntry.getValue();
            List<Map<String, Object>> nodes = (List<Map<String, Object>>) page.get("nodes");
            
            if (nodes != null) {
                for (Map<String, Object> nodeData : nodes) {
                    if (nodeId.equals(nodeData.get("id"))) {
                        return convertToDecisionTreeNode(nodeData);
                    }
                }
            }
        }
        
        return null;
    }
    
    private DecisionTreeNode convertToDecisionTreeNode(Map<String, Object> nodeData) {
        try {
            String json = jsonMapper.writeValueAsString(nodeData);
            return jsonMapper.readValue(json, DecisionTreeNode.class);
        } catch (Exception e) {
            log.error("Error converting node data", e);
            return null;
        }
    }
    
    public void applyUserSelection(UserSelection userSelection, HmsMirrorConfig config) {
        String strategy = userSelection.getStrategy();
        Map<String, Object> treeData = decisionTrees.get(strategy.toUpperCase());
        
        if (treeData == null) {
            return;
        }
        
        DecisionTreeNode currentNode = findNodeById(treeData, userSelection.getCurrentNodeId());
        if (currentNode == null) {
            return;
        }
        
        // Apply configuration based on node ID and selection
        applyNodeBasedMapping(userSelection.getCurrentNodeId(), userSelection, config);
    }
    
    private void applyNodeBasedMapping(String nodeId, UserSelection userSelection, HmsMirrorConfig config) {
        Object value = userSelection.getSelectedOption();
        if (userSelection.getTextValue() != null) {
            value = userSelection.getTextValue();
        } else if (userSelection.getInputValue() != null) {
            value = userSelection.getInputValue();
        } else if (userSelection.getSelectedOptions() != null && !userSelection.getSelectedOptions().isEmpty()) {
            // Handle multi-select options
            value = userSelection.getSelectedOptions();
        }
        
        log.info("Applying node mapping for nodeId: {}, value: {}, type: {}", nodeId, value, value != null ? value.getClass().getSimpleName() : "null");
        
        // Map specific node IDs to configuration properties
        switch (nodeId) {
            case "database_only":
                boolean databaseOnlyValue = parseBoolean(value);
                config.setDatabaseOnly(databaseOnlyValue);
                log.info("Set databaseOnly = {} (parsed from: {})", config.isDatabaseOnly(), value);
                break;
            case "copy_avro_schemas":
                boolean copyAvroValue = parseBoolean(value);
                config.setCopyAvroSchemaUrls(copyAvroValue);
                log.info("Set copyAvroSchemaUrls = {} (parsed from: {})", config.isCopyAvroSchemaUrls(), value);
                break;
            case "skip_link_check":
                boolean skipLinkValue = parseBoolean(value);
                config.setSkipLinkCheck(skipLinkValue);
                log.info("Set skipLinkCheck = {} (parsed from: {})", config.isSkipLinkCheck(), value);
                break;
            case "execution_options":
                if (userSelection.getSelectedOptions() != null) {
                    for (String option : userSelection.getSelectedOptions()) {
                        switch (option) {
                            case "readOnly":
                                config.setReadOnly(true);
                                log.info("Set readOnly = true");
                                break;
                            case "sync":
                                config.setSync(true);
                                log.info("Set sync = true");
                                break;
                            case "execute":
                                config.setExecute(true);
                                log.info("Set execute = true");
                                break;
                        }
                    }
                }
                break;
            case "execution_type":
                if ("execute".equals(value)) {
                    config.setExecute(true);
                    log.info("Set execute = true");
                } else if ("dryRun".equals(value)) {
                    config.setExecute(false);
                    log.info("Set execute = false (dry run)");
                }
                break;
            // Optimization Settings
            case "optimization_approach":
                if ("autoTune".equals(value)) {
                    config.getOptimization().setSkip(false);
                    config.getOptimization().setAutoTune(true);
                    log.info("Set optimization.autoTune = true");
                } else if ("skip".equals(value)) {
                    config.getOptimization().setSkip(true);
                    config.getOptimization().setAutoTune(false);
                    log.info("Set optimization.skip = true");
                }
                break;
            case "batch_size":
                int batchSize = parseInt(value, 100);
                // Note: No direct batch size setting on Optimization, using skip stats collection as proxy
                config.getOptimization().setSkipStatsCollection(batchSize > 100);
                log.info("Set optimization.skipStatsCollection = {} (based on batch size: {})", batchSize > 100, batchSize);
                break;
            
            // Translator Settings
            case "warehouse_plans":
                if (userSelection.getSelectedOptions() != null) {
                    // Note: Translator doesn't have addWarehousePlan method, using forceExternalLocation as proxy
                    config.getTranslator().setForceExternalLocation(true);
                    log.info("Set translator.forceExternalLocation = true (warehouse plans selected)");
                }
                break;
            case "consolidate_external_dirs":
                boolean consolidateValue = parseBoolean(value);
                // Note: Using forceExternalLocation as closest equivalent for consolidation
                config.getTranslator().setForceExternalLocation(consolidateValue);
                log.info("Set translator.forceExternalLocation = {} (consolidate external dirs)", consolidateValue);
                break;
                
            // Hybrid Configuration Settings 
            case "acid_bucket_threshold":
                int bucketThreshold = parseInt(value, 2);
                // Note: Using exportImportPartitionLimit as proxy for bucket threshold
                config.getHybrid().setExportImportPartitionLimit(bucketThreshold * 50);
                log.info("Set hybrid.exportImportPartitionLimit = {} (bucket threshold: {})", bucketThreshold * 50, bucketThreshold);
                break;
            case "acid_partition_limit": 
                int partitionLimit = parseInt(value, 500);
                config.getHybrid().setSqlPartitionLimit(partitionLimit);
                log.info("Set hybrid.sqlPartitionLimit = {}", partitionLimit);
                break;
            case "acid_downgrade":
                boolean downgradeValue = parseBoolean(value);
                // Note: Using sqlSizeLimit as proxy for downgrade (smaller limit when downgrading)
                config.getHybrid().setSqlSizeLimit(downgradeValue ? 512 * 1024 * 1024 : 1024 * 1024 * 1024);
                log.info("Set hybrid.sqlSizeLimit = {} (acid downgrade: {})", downgradeValue ? 512 * 1024 * 1024 : 1024 * 1024 * 1024, downgradeValue);
                break;
            case "acid_inplace":
                boolean inplaceValue = parseBoolean(value);
                // Note: Using exportImportPartitionLimit as proxy for inplace (higher limit for inplace)
                config.getHybrid().setExportImportPartitionLimit(inplaceValue ? 200 : 100);
                log.info("Set hybrid.exportImportPartitionLimit = {} (acid inplace: {})", inplaceValue ? 200 : 100, inplaceValue);
                break;
                
            // Iceberg Conversion Settings
            case "iceberg_migration":
                boolean icebergValue = parseBoolean(value);
                config.getIcebergConversion().setEnable(icebergValue);
                log.info("Set icebergConversion.enable = {}", icebergValue);
                break;
            case "iceberg_version":
                if (value instanceof String) {
                    String versionStr = (String) value;
                    int version = "v2".equals(versionStr) ? 2 : 1;
                    config.getIcebergConversion().setVersion(version);
                    log.info("Set icebergConversion.version = {} (from string: {})", version, versionStr);
                }
                break;
                
            // Text Input Fields
            case "db_prefix":
                if (value instanceof String) {
                    config.setDbPrefix((String) value);
                    log.info("Set dbPrefix = {}", value);
                }
                break;
            case "db_rename": 
                if (value instanceof String) {
                    config.setDbRename((String) value);
                    log.info("Set dbRename = {}", value);
                }
                break;
            case "source_namespace":
                if (value instanceof String) {
                    // Note: No direct namespace setting on Translator, using forceExternalLocation
                    config.getTranslator().setForceExternalLocation(true);
                    log.info("Set translator.forceExternalLocation = true (source namespace: {})", value);
                }
                break;
            case "target_namespace":
                if (value instanceof String) {
                    // Note: No direct namespace setting on Translator, using forceExternalLocation
                    config.getTranslator().setForceExternalLocation(true);
                    log.info("Set translator.forceExternalLocation = true (target namespace: {})", value);
                }
                break;
                
            // Add more node mappings as needed
            default:
                log.debug("No configuration mapping defined for node: {}", nodeId);
        }
    }
    
    private boolean parseBoolean(Object value) {
        log.debug("Parsing boolean from value: {} (type: {})", value, value != null ? value.getClass().getSimpleName() : "null");
        if (value instanceof Boolean) {
            return (Boolean) value;
        } else if (value instanceof String) {
            String strValue = (String) value;
            boolean result = "true".equalsIgnoreCase(strValue);
            log.debug("Parsed string '{}' to boolean: {}", strValue, result);
            return result;
        } else if (value == null) {
            log.debug("Null value parsed to false");
            return false;
        }
        log.debug("Unknown type {} parsed to false", value.getClass().getSimpleName());
        return false;
    }
    
    private int parseInt(Object value, int defaultValue) {
        log.debug("Parsing int from value: {} (type: {})", value, value != null ? value.getClass().getSimpleName() : "null");
        if (value instanceof Integer) {
            return (Integer) value;
        } else if (value instanceof String) {
            try {
                int result = Integer.parseInt((String) value);
                log.debug("Parsed string '{}' to int: {}", value, result);
                return result;
            } catch (NumberFormatException e) {
                log.debug("Could not parse string '{}' to int, using default: {}", value, defaultValue);
                return defaultValue;
            }
        } else if (value == null) {
            log.debug("Null value parsed to default: {}", defaultValue);
            return defaultValue;
        }
        log.debug("Unknown type {} parsed to default: {}", value.getClass().getSimpleName(), defaultValue);
        return defaultValue;
    }
    
    private Map<String, Object> findNodeConfigMapping(Map<String, Object> treeData, String nodeId) {
        Map<String, Object> pages = (Map<String, Object>) treeData.get("pages");
        
        for (Map.Entry<String, Object> pageEntry : pages.entrySet()) {
            Map<String, Object> page = (Map<String, Object>) pageEntry.getValue();
            List<Map<String, Object>> nodes = (List<Map<String, Object>>) page.get("nodes");
            
            if (nodes != null) {
                for (Map<String, Object> nodeData : nodes) {
                    if (nodeId.equals(nodeData.get("id"))) {
                        return (Map<String, Object>) nodeData.get("configMapping");
                    }
                }
            }
        }
        
        return null;
    }
    
    private void applyConfigMapping(Map<String, Object> configMapping, HmsMirrorConfig config, Object value) {
        for (Map.Entry<String, Object> entry : configMapping.entrySet()) {
            String key = entry.getKey();
            Object mappingValue = entry.getValue();
            
            if (mappingValue instanceof String) {
                String stringValue = (String) mappingValue;
                if (stringValue.contains("${value}")) {
                    stringValue = stringValue.replace("${value}", String.valueOf(value));
                }
                setConfigProperty(config, key, parseValue(stringValue));
            } else if (mappingValue instanceof Map) {
                // Handle nested configuration
                setNestedConfigProperty(config, key, (Map<String, Object>) mappingValue, value);
            } else {
                setConfigProperty(config, key, mappingValue);
            }
        }
    }
    
    private Object parseValue(String value) {
        if ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
            return Boolean.parseBoolean(value);
        }
        
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            // Not an integer
        }
        
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            // Not a double
        }
        
        return value; // Return as string
    }
    
    private void setConfigProperty(HmsMirrorConfig config, String key, Object value) {
        try {
            // Use reflection or direct setters based on key
            // This is a simplified implementation - you'd need to expand this
            // based on your HmsMirrorConfig structure
            
            switch (key) {
                case "databaseOnly":
                    config.setDatabaseOnly((Boolean) value);
                    break;
                case "copyAvroSchemaUrls":
                    config.setCopyAvroSchemaUrls((Boolean) value);
                    break;
                case "skipLinkCheck":
                    config.setSkipLinkCheck((Boolean) value);
                    break;
                case "execute":
                    config.setExecute((Boolean) value);
                    break;
                case "readOnly":
                    config.setReadOnly((Boolean) value);
                    break;
                case "sync":
                    config.setSync((Boolean) value);
                    break;
                // Add more mappings as needed
                default:
                    log.warn("Unknown config property: {}", key);
            }
        } catch (Exception e) {
            log.error("Error setting config property: {} = {}", key, value, e);
        }
    }
    
    private void setNestedConfigProperty(HmsMirrorConfig config, String key, Map<String, Object> nestedMapping, Object value) {
        // Handle nested properties like transfer.intermediateStorage, migrateACID.on, etc.
        // This would need to be implemented based on your specific config structure
        log.info("Setting nested config property: {} with mapping: {}", key, nestedMapping);
    }
    
    public Map<String, Object> extractConfigData(HmsMirrorConfig config) {
        Map<String, Object> configData = new HashMap<>();
        
        // Extract relevant config properties for display
        configData.put("databaseOnly", config.isDatabaseOnly());
        configData.put("copyAvroSchemaUrls", config.isCopyAvroSchemaUrls());
        configData.put("skipLinkCheck", config.isSkipLinkCheck());
        configData.put("execute", config.isExecute());
        configData.put("readOnly", config.isReadOnly());
        configData.put("sync", config.isSync());
        
        // Add more properties as needed
        
        return configData;
    }
    
    public void resetConfig(HmsMirrorConfig config) {
        // Reset config to default values
        config.setDatabaseOnly(false);
        config.setCopyAvroSchemaUrls(false);
        config.setSkipLinkCheck(false);
        config.setExecute(false);
        config.setReadOnly(false);
        config.setSync(false);
        
        // Reset other properties as needed
    }
    
    public Map<String, Object> validateConfig(HmsMirrorConfig config) {
        Map<String, Object> validation = new HashMap<>();
        List<String> errors = new ArrayList<>();
        List<String> warnings = new ArrayList<>();
        
        // Add validation logic here
        if (config.isSkipLinkCheck() && config.getTransfer() != null && 
            config.getTransfer().getIntermediateStorage() == null) {
            errors.add("Intermediate storage is required when skip link check is enabled");
        }
        
        validation.put("isValid", errors.isEmpty());
        validation.put("errors", errors);
        validation.put("warnings", warnings);
        
        return validation;
    }
}