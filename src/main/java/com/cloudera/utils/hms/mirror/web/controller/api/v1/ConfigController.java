/*
 * Copyright (c) 2024. Cloudera, Inc. All Rights Reserved
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

import com.cloudera.utils.hms.mirror.Environment;
import com.cloudera.utils.hms.mirror.datastrategy.DataStrategyEnum;
import com.cloudera.utils.hms.mirror.domain.*;
import com.cloudera.utils.hms.mirror.service.HmsMirrorCfgService;
import com.cloudera.utils.hms.mirror.web.service.ConfigService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.validation.constraints.NotNull;
import java.util.*;

@RestController
@Slf4j
@RequestMapping(path = "/api/v1/config")
public class ConfigController {

    private ConfigService configService;
    private HmsMirrorCfgService hmsMirrorCfgService;

    @Autowired
    public void setConfigService(ConfigService configService) {
        this.configService = configService;
    }

    @Autowired
    public void setHmsMirrorCfgService(HmsMirrorCfgService hmsMirrorCfgService) {
        this.hmsMirrorCfgService = hmsMirrorCfgService;
    }

    @Operation(summary = "Get a list of available configs")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Found list of Configs",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = List.class))})
//            , @ApiResponse(responseCode = "400", description = "Invalid id supplied",
//                    content = @Content)
//            , @ApiResponse(responseCode = "404", description = "Config not found",
//                    content = @Content)
    })
    @ResponseBody
    @RequestMapping(method = RequestMethod.GET, value = "/list")
    public List<String> getConfigList() {
        log.info("Getting Config List");
        return configService.getConfigList();
    }

    @Operation(summary = "Get the current config")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Current Config",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = HmsMirrorConfig.class))})
//            , @ApiResponse(responseCode = "400", description = "Invalid id supplied",
//                    content = @Content)
//            , @ApiResponse(responseCode = "404", description = "Config not found",
//                    content = @Content)
    })
    @ResponseBody
    @RequestMapping(method = RequestMethod.GET, value = "/")
    public HmsMirrorConfig getConfig() {
        log.info("Getting Current Config");
        return configService.getCurrentConfig();
    }

    @Operation(summary = "Get the config by id")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Found the Catalog",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = HmsMirrorConfig.class))}),
            @ApiResponse(responseCode = "400", description = "Invalid id supplied",
                    content = @Content),
            @ApiResponse(responseCode = "404", description = "Config not found",
                    content = @Content)})
    @ResponseBody
    @RequestMapping(method = RequestMethod.GET, value = "/{id}")
    public HmsMirrorConfig getById(@PathVariable @NotNull String id) {
        log.info("Getting Config by id: {}", id);
        return null;
    }

    @Operation(summary = "Load a config by id")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Config loaded",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = HmsMirrorConfig.class))}),
            @ApiResponse(responseCode = "400", description = "Invalid input",
                    content = @Content)})
    @ResponseBody
    @RequestMapping(method = RequestMethod.POST, value = "/load/{id}")
    public HmsMirrorConfig load(@PathVariable @NotNull String id) {
        log.info("Loading Config by id: {}", id);
        return hmsMirrorCfgService.loadConfig(id);
    }

    @Operation(summary = "Save current config to id")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Config Saved",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = Boolean.class))}),
            @ApiResponse(responseCode = "400", description = "Invalid input",
                    content = @Content)})
    @ResponseBody
    @RequestMapping(method = RequestMethod.POST, value = "/save/{id}")
    public boolean save(@PathVariable @NotNull String id) {
        log.info("Save current config to: {}", id);
        HmsMirrorConfig config = hmsMirrorCfgService.getHmsMirrorConfig();
        return hmsMirrorCfgService.saveConfig(config, id);
    }

    @Operation(summary = "Get the configs clusters")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Found the Clusters",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = Map.class))}),
            @ApiResponse(responseCode = "400", description = "Invalid id supplied",
                    content = @Content),
            @ApiResponse(responseCode = "404", description = "Cluster not found",
                    content = @Content)})
    @ResponseBody
    @RequestMapping(method = RequestMethod.GET, value = "/clusters")
    public Map<Environment, Cluster> getClusters() {
        log.info("Getting Clusters for current Config");
        Map<Environment, Cluster> clusters = hmsMirrorCfgService.getHmsMirrorConfig().getClusters();
        return clusters;
    }

    @Operation(summary = "Get the LEFT|RIGHT cluster config")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Found the Clusters",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = Cluster.class))}),
            @ApiResponse(responseCode = "400", description = "Invalid id supplied",
                    content = @Content),
            @ApiResponse(responseCode = "404", description = "Cluster not found",
                    content = @Content)})
    @ResponseBody
    @RequestMapping(method = RequestMethod.GET, value = "/clusters/{side}")
    public Cluster getCluster(@PathVariable @NotNull String side) {
        log.info("Getting {} Cluster current Config", side);
        String sideStr = side.toUpperCase();
        Environment env = Environment.valueOf(sideStr);
        Cluster cluster = hmsMirrorCfgService.getHmsMirrorConfig().getClusters().get(env);
        return cluster;
    }

    /**
     * Complete the implementation for getting details for the current config.
     * 1. filter
     * 2. ...
     **/

    // Setting the current config values.
    @Operation(summary = "Set the Data Strategy")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Data Strategy set",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = DataStrategyEnum.class))}),
            @ApiResponse(responseCode = "400", description = "Invalid DataStrategy supplied",
                    content = @Content),
            @ApiResponse(responseCode = "404", description = "DataStrategy not found",
                    content = @Content)})
    @ResponseBody
    @RequestMapping(method = RequestMethod.PUT, value = "/datastrategy")
    public DataStrategyEnum setDataStrategy(@RequestParam("value") String value) {
        log.info("Setting Data Strategy to: {}", value);
        String valueStr = value.toUpperCase();
        DataStrategyEnum dataStrategyEnum = DataStrategyEnum.valueOf(valueStr);
        hmsMirrorCfgService.getHmsMirrorConfig().setDataStrategy(dataStrategyEnum);
        return dataStrategyEnum;
    }

    @Operation(summary = "Set the Databases.  Comma Separated List of Metastore Database names")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Databases set",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = List.class))}),
            @ApiResponse(responseCode = "400", description = "Invalid Database list",
                    content = @Content)})
    @ResponseBody
    @RequestMapping(method = RequestMethod.PUT, value = "/databases")
    public List<String> setDatabases(@RequestParam("value") String value) {
        log.info("Setting Databases to: {}", value);
        String[] dbs = value.split(",");
        hmsMirrorCfgService.getHmsMirrorConfig().setDatabases(dbs);
        return Arrays.asList(hmsMirrorCfgService.getHmsMirrorConfig().getDatabases());
    }

    @Operation(summary = "Set the Database Filters")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Databases set",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = Filter.class))}),
            @ApiResponse(responseCode = "400", description = "Invalid Database list",
                    content = @Content)})
    @ResponseBody
    @RequestMapping(method = RequestMethod.PUT, value = "/filter")
    public Filter setDatabaseFilter(@RequestParam(value = "tblExcludeRegEx", required = false) String excludeRegEx,
                                    @RequestParam(value = "tblRegEx", required = false) String regEx,
                                    @RequestParam(value = "tblSizeLimit", required = false) String tblSizeLimit,
                                    @RequestParam(value = "tblPartitionLimit", required = false) String tblPartitionLimit) {
        if (excludeRegEx != null) {
            log.info("Setting Table Exclude RegEx to: {}", excludeRegEx);
            hmsMirrorCfgService.getHmsMirrorConfig().getFilter().setTblExcludeRegEx(excludeRegEx);
        }
        if (regEx != null) {
            log.info("Setting Table RegEx to: {}", regEx);
            hmsMirrorCfgService.getHmsMirrorConfig().getFilter().setTblRegEx(regEx);
        }
        if (tblSizeLimit != null) {
            log.info("Setting Table Size Limit to: {}", tblSizeLimit);
            hmsMirrorCfgService.getHmsMirrorConfig().getFilter().setTblSizeLimit(Long.parseLong(tblSizeLimit));
        }
        if (tblPartitionLimit != null) {
            log.info("Setting Table Partition Limit to: {}", tblPartitionLimit);
            hmsMirrorCfgService.getHmsMirrorConfig().getFilter().setTblPartitionLimit(Integer.parseInt(tblPartitionLimit));
        }
        return hmsMirrorCfgService.getHmsMirrorConfig().getFilter();
    }

    @Operation(summary = "Set Evaluate Partition Locations")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Evaluated Partition Locations set",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = Boolean.class))})
    })
    @ResponseBody
    @RequestMapping(method = RequestMethod.PUT, value = "/evaluatePartitionLocation")
    public Boolean setEvaluatePartitionLocation(@RequestParam("value") String value) {
        log.info("Setting Evaluate Partition Location to: {}", value);
        Boolean boolValue = Boolean.parseBoolean(value);
        hmsMirrorCfgService.getHmsMirrorConfig().setEvaluatePartitionLocation(boolValue);
        return hmsMirrorCfgService.getHmsMirrorConfig().isEvaluatePartitionLocation();
    }


    @Operation(summary = "Set Execute")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Execute set",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = Boolean.class))})
    })
    @ResponseBody
    @RequestMapping(method = RequestMethod.PUT, value = "/execute")
    public Boolean setExecute(@RequestParam("value") String value) {
        log.info("Setting Execute to: {}", value);
        Boolean boolValue = Boolean.parseBoolean(value);
        hmsMirrorCfgService.getHmsMirrorConfig().setExecute(boolValue);
        return hmsMirrorCfgService.getHmsMirrorConfig().isExecute();
    }

    @Operation(summary = "Set the ACID Migrations")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "ACID Migrations set",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = MigrateACID.class))}),
            @ApiResponse(responseCode = "400", description = "Invalid Database list",
                    content = @Content)})
    @ResponseBody
    @RequestMapping(method = RequestMethod.PUT, value = "/migrateACID")
    public MigrateACID setDatabaseFilter(@RequestParam(value = "on", required = false) Boolean on,
                                    @RequestParam(value = "only", required = false) Boolean only,
                                    @RequestParam(value = "artificialBucketThreshold", required = false) String artificialBucketThreshold,
                                    @RequestParam(value = "partitionLimit", required = false) String partitionLimit,
                                    @RequestParam(value = "downgrade", required = false) Boolean downgrade,
                                    @RequestParam(value = "inplace", required = false) Boolean inplace
                                    ) {
        if (on != null) {
            log.info("Setting Migrate ACID 'on' to: {}", on);
            hmsMirrorCfgService.getHmsMirrorConfig().getMigrateACID().setOn(on);
        }
        if (only != null) {
            log.info("Setting Migrate ACID 'only' to: {}", only);
            hmsMirrorCfgService.getHmsMirrorConfig().getMigrateACID().setOnly(only);
        }
        if (artificialBucketThreshold != null) {
            log.info("Setting Migrate ACID 'artificialBucketThreshold' to: {}", artificialBucketThreshold);
            hmsMirrorCfgService.getHmsMirrorConfig().getMigrateACID().setArtificialBucketThreshold(Integer.parseInt(artificialBucketThreshold));
        }
        if (partitionLimit != null) {
            log.info("Setting Migrate ACID 'partitionLimit' to: {}", partitionLimit);
            hmsMirrorCfgService.getHmsMirrorConfig().getMigrateACID().setPartitionLimit(Integer.parseInt(partitionLimit));
        }
        if (downgrade != null) {
            log.info("Setting Migrate ACID 'downgrade' to: {}", downgrade);
            hmsMirrorCfgService.getHmsMirrorConfig().getMigrateACID().setDowngrade(downgrade);
        }
        if (inplace != null) {
            log.info("Setting Migrate ACID 'inplace' to: {}", inplace);
            hmsMirrorCfgService.getHmsMirrorConfig().getMigrateACID().setInplace(inplace);
        }
        return hmsMirrorCfgService.getHmsMirrorConfig().getMigrateACID();
    }

    @Operation(summary = "Set Report Output Directory")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Report Output Directory set",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = String.class))})
    })
    @ResponseBody
    @RequestMapping(method = RequestMethod.PUT, value = "/outputDirectory")
    public String setOutputDirectory(@RequestParam("value") String value) {
        log.info("Setting Output Directory to: {}", value);
        hmsMirrorCfgService.getHmsMirrorConfig().setOutputDirectory(value);
        return hmsMirrorCfgService.getHmsMirrorConfig().getOutputDirectory();
    }

    // Transfer
    @Operation(summary = "Set Transfer Details")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Transfer details set",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = TransferConfig.class))})})
    @ResponseBody
    @RequestMapping(method = RequestMethod.PUT, value = "/transfer")
    public TransferConfig setTransfer(@RequestParam(value = "concurrency", required = false) Integer concurrency,
                                         @RequestParam(value = "transferPrefix", required = false) String transferPrefix,
                                         @RequestParam(value = "shadowPrefix", required = false) String shadowPrefix,
                                         @RequestParam(value = "exportBaseDirPrefix", required = false) String exportBaseDirPrefix,
                                         @RequestParam(value = "remoteWorkingDirectory", required = false) String remoteWorkingDirectory,
                                         @RequestParam(value = "intermediateStorage", required = false) String intermediateStorage,
                                         @RequestParam(value = "commonStorage", required = false) String commonStorage
                                         ) {
        if (concurrency != null) {
            log.info("Setting Transfer 'concurrency' to: {}", concurrency);
            hmsMirrorCfgService.getHmsMirrorConfig().getTransfer().setConcurrency(concurrency);
        }
        if (transferPrefix != null) {
            log.info("Setting Transfer 'transferPrefix' to: {}", transferPrefix);
            hmsMirrorCfgService.getHmsMirrorConfig().getTransfer().setTransferPrefix(transferPrefix);
        }
        if (shadowPrefix != null) {
            log.info("Setting Transfer 'shadowPrefix' to: {}", shadowPrefix);
            hmsMirrorCfgService.getHmsMirrorConfig().getTransfer().setShadowPrefix(shadowPrefix);
        }
        if (exportBaseDirPrefix != null) {
            log.info("Setting Transfer 'exportBaseDirPrefix' to: {}", exportBaseDirPrefix);
            hmsMirrorCfgService.getHmsMirrorConfig().getTransfer().setExportBaseDirPrefix(exportBaseDirPrefix);
        }
        if (remoteWorkingDirectory != null) {
            log.info("Setting Transfer 'remoteWorkingDirectory' to: {}", remoteWorkingDirectory);
            hmsMirrorCfgService.getHmsMirrorConfig().getTransfer().setRemoteWorkingDirectory(remoteWorkingDirectory);
        }
        if (intermediateStorage != null) {
            log.info("Setting Transfer 'intermediateStorage' to: {}", intermediateStorage);
            hmsMirrorCfgService.getHmsMirrorConfig().getTransfer().setIntermediateStorage(intermediateStorage);
        }
        if (commonStorage != null) {
            log.info("Setting Transfer 'commonStorage' to: {}", commonStorage);
            hmsMirrorCfgService.getHmsMirrorConfig().getTransfer().setCommonStorage(commonStorage);
        }
        return hmsMirrorCfgService.getHmsMirrorConfig().getTransfer();
    }

    // Transfer / Warehouse
    @Operation(summary = "Set Warehouse Details")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Warehouse Details set",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = WarehouseConfig.class))})})
    @ResponseBody
    @RequestMapping(method = RequestMethod.PUT, value = "/transfer/warehouse")
    public WarehouseConfig setTransferWarehouse(
                                         @RequestParam(value = "managedDirectory", required = false) String managedDirectory,
                                         @RequestParam(value = "externalDirectory", required = false) String externalDirectory
                                         ) {
        if (managedDirectory != null) {
            log.info("Setting Warehouse 'managedDirectory' to: {}", managedDirectory);
            hmsMirrorCfgService.getHmsMirrorConfig().getTransfer().getWarehouse().setManagedDirectory(managedDirectory);
        }
        if (externalDirectory != null) {
            log.info("Setting Warehouse 'externalDirectory' to: {}", externalDirectory);
            hmsMirrorCfgService.getHmsMirrorConfig().getTransfer().getWarehouse().setExternalDirectory(externalDirectory);
        }
        return hmsMirrorCfgService.getHmsMirrorConfig().getTransfer().getWarehouse();
    }

    // Transfer / Storage Migration
    @Operation(summary = "Set Storage Migration Details")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Storage Migration Details set",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = StorageMigration.class))})})
    @ResponseBody
    @RequestMapping(method = RequestMethod.PUT, value = "/transfer/storageMigration")
    public StorageMigration setStorageMigration(
            @RequestParam(value = "strategy", required = false) DataStrategyEnum strategy,
            @RequestParam(value = "distcp", required = false) Boolean distcp,
            @RequestParam(value = "dataFlow", required = false)  DistcpFlow dataFlow
    ) {
        if (strategy != null) {
            log.info("Setting Storage Migration 'strategy' to: {}", strategy);
            hmsMirrorCfgService.getHmsMirrorConfig().getTransfer().getStorageMigration().setStrategy(strategy);
        }
        if (distcp != null) {
            log.info("Setting Storage Migration 'distcp' to: {}", distcp);
            hmsMirrorCfgService.getHmsMirrorConfig().getTransfer().getStorageMigration().setDistcp(distcp);
        }
        if (dataFlow != null) {
            log.info("Setting Storage Migration 'dataFlow' to: {}", dataFlow);
            hmsMirrorCfgService.getHmsMirrorConfig().getTransfer().getStorageMigration().setDataFlow(dataFlow);
        }
        return hmsMirrorCfgService.getHmsMirrorConfig().getTransfer().getStorageMigration();
    }


}
