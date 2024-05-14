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

package com.cloudera.utils.hms.mirror.web.controller.api.v1.config;

import com.cloudera.utils.hms.mirror.Environment;
import com.cloudera.utils.hms.mirror.datastrategy.DataStrategyEnum;
import com.cloudera.utils.hms.mirror.domain.*;
import com.cloudera.utils.hms.mirror.service.ConfigService;
import com.cloudera.utils.hms.mirror.service.ExecuteSessionService;
import com.cloudera.utils.hms.mirror.web.service.WebConfigService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.validation.constraints.NotNull;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@RestController
@Slf4j
@RequestMapping(path = "/api/v1/config")
public class ConfigController {

    private ConfigService configService2;
    private WebConfigService webConfigService;
    private ExecuteSessionService executeSessionService;

    @Autowired
    public void setConfigService(ConfigService configService) {
        this.configService2 = configService;
    }

    @Autowired
    public void setWebConfigService(WebConfigService webConfigService) {
        this.webConfigService = webConfigService;
    }

    @Autowired
    public void setHmsMirrorCfgService(ExecuteSessionService executeSessionService) {
        this.executeSessionService = executeSessionService;
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
        return webConfigService.getConfigList();
    }

    @Operation(summary = "Get session config")
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
    public HmsMirrorConfig getConfig(@RequestParam(name = "sessionId", required = false) String sessionId) {
        log.info("Getting Config: {}", sessionId);
        return executeSessionService.getSession(sessionId).getHmsMirrorConfig();
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
        return configService2.loadConfig(id);
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
    public HmsMirrorConfig load(@RequestParam(name = "sessionId", required = false) String sessionId,
                                @PathVariable @NotNull String id) {
        log.info("{}: Loading Config by id: {}", sessionId, id);
        HmsMirrorConfig config = configService2.loadConfig(id);
        executeSessionService.getSession(sessionId).setHmsMirrorConfig(config);
        return config;
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
    public boolean save(@RequestParam(name = "sessionId", required = false) String sessionId,
                        @PathVariable @NotNull String id) {
        log.info("{}: Save current config to: {}", sessionId, id);
        HmsMirrorConfig config = executeSessionService.getSession(sessionId).getHmsMirrorConfig();
        return configService2.saveConfig(config, id);
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
    public Map<Environment, Cluster> getClusters(@RequestParam(name = "sessionId", required = false) String sessionId) {
        log.info("{}: Getting Clusters for current Config", sessionId);
        return executeSessionService.getSession(sessionId).getHmsMirrorConfig().getClusters();
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
    public Cluster getCluster(@RequestParam(name = "sessionId", required = false) String sessionId,
                              @PathVariable @NotNull String side) {
        log.info("{}: Getting {} Cluster current Config", sessionId, side);
        String sideStr = side.toUpperCase();
        Environment env = Environment.valueOf(sideStr);
        return executeSessionService.getSession(sessionId).getHmsMirrorConfig().getClusters().get(env);
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
    public DataStrategyEnum setDataStrategy(@RequestParam(name = "sessionId", required = false) String sessionId,
                                            @RequestParam("value") String value) {
        log.info("{}: Setting Data Strategy to: {}", sessionId, value);
        String valueStr = value.toUpperCase();
        DataStrategyEnum dataStrategyEnum = DataStrategyEnum.valueOf(valueStr);
        executeSessionService.getSession(sessionId).getHmsMirrorConfig().setDataStrategy(dataStrategyEnum);
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
    public List<String> setDatabases(@RequestParam(name = "sessionId", required = false) String sessionId,
                                     @RequestParam("value") String value) {
        log.info("{}: Setting Databases to: {}", sessionId, value);
        String[] dbs = value.split(",");
        executeSessionService.getSession(sessionId).getHmsMirrorConfig().setDatabases(dbs);
        return Arrays.asList(executeSessionService.getSession(sessionId).getHmsMirrorConfig().getDatabases());
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
    public Filter setDatabaseFilter(@RequestParam(name = "sessionId", required = false) String sessionId,
                                    @RequestParam(value = "tblExcludeRegEx", required = false) String excludeRegEx,
                                    @RequestParam(value = "tblRegEx", required = false) String regEx,
                                    @RequestParam(value = "tblSizeLimit", required = false) String tblSizeLimit,
                                    @RequestParam(value = "tblPartitionLimit", required = false) String tblPartitionLimit) {
        if (excludeRegEx != null) {
            log.info("{}: Setting Table Exclude RegEx to: {}", sessionId, excludeRegEx);
            executeSessionService.getSession(sessionId).getHmsMirrorConfig().getFilter().setTblExcludeRegEx(excludeRegEx);
        }
        if (regEx != null) {
            log.info("{}: Setting Table RegEx to: {}", sessionId, regEx);
            executeSessionService.getSession(sessionId).getHmsMirrorConfig().getFilter().setTblRegEx(regEx);
        }
        if (tblSizeLimit != null) {
            log.info("{}: Setting Table Size Limit to: {}", sessionId, tblSizeLimit);
            executeSessionService.getSession(sessionId).getHmsMirrorConfig().getFilter().setTblSizeLimit(Long.parseLong(tblSizeLimit));
        }
        if (tblPartitionLimit != null) {
            log.info("{}: Setting Table Partition Limit to: {}", sessionId, tblPartitionLimit);
            executeSessionService.getSession(sessionId).getHmsMirrorConfig().getFilter().setTblPartitionLimit(Integer.parseInt(tblPartitionLimit));
        }
        return executeSessionService.getSession(sessionId).getHmsMirrorConfig().getFilter();
    }

    @Operation(summary = "Set Evaluate Partition Locations")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Evaluated Partition Locations set",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = Boolean.class))})
    })
    @ResponseBody
    @RequestMapping(method = RequestMethod.PUT, value = "/evaluatePartitionLocation")
    public Boolean setEvaluatePartitionLocation(@RequestParam(name = "sessionId", required = false) String sessionId,
                                                @RequestParam("value") String value) {
        log.info("{}: Setting Evaluate Partition Location to: {}", sessionId, value);
        boolean boolValue = Boolean.parseBoolean(value);
        executeSessionService.getSession(sessionId).getHmsMirrorConfig().setEvaluatePartitionLocation(boolValue);
        return executeSessionService.getSession(sessionId).getHmsMirrorConfig().isEvaluatePartitionLocation();
    }


//    @Operation(summary = "Set Execute")
//    @ApiResponses(value = {
//            @ApiResponse(responseCode = "200", description = "Execute set",
//                    content = {@Content(mediaType = "application/json",
//                            schema = @Schema(implementation = Boolean.class))})
//    })
//    @ResponseBody
//    @RequestMapping(method = RequestMethod.PUT, value = "/execute")
//    public Boolean setExecute(@RequestParam("value") String value) {
//        log.info("Setting Execute to: {}", value);
//        Boolean boolValue = Boolean.parseBoolean(value);
//        executeSessionService.getHmsMirrorConfig().setExecute(boolValue);
//        return executeSessionService.getHmsMirrorConfig().isExecute();
//    }

    @Operation(summary = "Set the ACID Migrations")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "ACID Migrations set",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = MigrateACID.class))}),
            @ApiResponse(responseCode = "400", description = "Invalid Database list",
                    content = @Content)})
    @ResponseBody
    @RequestMapping(method = RequestMethod.PUT, value = "/migrateACID")
    public MigrateACID setDatabaseFilter(@RequestParam(name = "sessionId", required = false) String sessionId,
                                         @RequestParam(value = "on", required = false) Boolean on,
                                         @RequestParam(value = "only", required = false) Boolean only,
                                         @RequestParam(value = "artificialBucketThreshold", required = false) String artificialBucketThreshold,
                                         @RequestParam(value = "partitionLimit", required = false) String partitionLimit,
                                         @RequestParam(value = "downgrade", required = false) Boolean downgrade,
                                         @RequestParam(value = "inplace", required = false) Boolean inplace
    ) {
        if (on != null) {
            log.info("{}: Setting Migrate ACID 'on' to: {}", sessionId, on);
            executeSessionService.getSession(sessionId).getHmsMirrorConfig().getMigrateACID().setOn(on);
        }
        if (only != null) {
            log.info("{}: Setting Migrate ACID 'only' to: {}", sessionId, only);
            executeSessionService.getSession(sessionId).getHmsMirrorConfig().getMigrateACID().setOnly(only);
        }
        if (artificialBucketThreshold != null) {
            log.info("{}: Setting Migrate ACID 'artificialBucketThreshold' to: {}", sessionId, artificialBucketThreshold);
            executeSessionService.getSession(sessionId).getHmsMirrorConfig().getMigrateACID().setArtificialBucketThreshold(Integer.parseInt(artificialBucketThreshold));
        }
        if (partitionLimit != null) {
            log.info("{}: Setting Migrate ACID 'partitionLimit' to: {}", sessionId, partitionLimit);
            executeSessionService.getSession(sessionId).getHmsMirrorConfig().getMigrateACID().setPartitionLimit(Integer.parseInt(partitionLimit));
        }
        if (downgrade != null) {
            log.info("{}: Setting Migrate ACID 'downgrade' to: {}", sessionId, downgrade);
            executeSessionService.getSession(sessionId).getHmsMirrorConfig().getMigrateACID().setDowngrade(downgrade);
        }
        if (inplace != null) {
            log.info("{}: Setting Migrate ACID 'inplace' to: {}", sessionId, inplace);
            executeSessionService.getSession(sessionId).getHmsMirrorConfig().getMigrateACID().setInplace(inplace);
        }
        return executeSessionService.getSession(sessionId).getHmsMirrorConfig().getMigrateACID();
    }

    @Operation(summary = "Set Report Output Directory")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Report Output Directory set",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = String.class))})
    })
    @ResponseBody
    @RequestMapping(method = RequestMethod.PUT, value = "/outputDirectory")
    public String setOutputDirectory(@RequestParam(name = "sessionId", required = false) String sessionId,
                                     @RequestParam("value") String value) {
        log.info("{}: Setting Output Directory to: {}", sessionId, value);
        executeSessionService.getSession(sessionId).getHmsMirrorConfig().setOutputDirectory(value);
        return executeSessionService.getSession(sessionId).getHmsMirrorConfig().getOutputDirectory();
    }

    // Transfer
    @Operation(summary = "Set Transfer Details")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Transfer details set",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = TransferConfig.class))})})
    @ResponseBody
    @RequestMapping(method = RequestMethod.PUT, value = "/transfer")
    public TransferConfig setTransfer(@RequestParam(name = "sessionId", required = false) String sessionId,
                                      @RequestParam(value = "concurrency", required = false) Integer concurrency,
                                      @RequestParam(value = "transferPrefix", required = false) String transferPrefix,
                                      @RequestParam(value = "shadowPrefix", required = false) String shadowPrefix,
                                      @RequestParam(value = "exportBaseDirPrefix", required = false) String exportBaseDirPrefix,
                                      @RequestParam(value = "remoteWorkingDirectory", required = false) String remoteWorkingDirectory,
                                      @RequestParam(value = "intermediateStorage", required = false) String intermediateStorage,
                                      @RequestParam(value = "commonStorage", required = false) String commonStorage
    ) {
        if (concurrency != null) {
            log.info("{}: Setting Transfer 'concurrency' to: {}", sessionId, concurrency);
            executeSessionService.getSession(sessionId).getHmsMirrorConfig().getTransfer().setConcurrency(concurrency);
        }
        if (transferPrefix != null) {
            log.info("{}: Setting Transfer 'transferPrefix' to: {}", sessionId, transferPrefix);
            executeSessionService.getSession(sessionId).getHmsMirrorConfig().getTransfer().setTransferPrefix(transferPrefix);
        }
        if (shadowPrefix != null) {
            log.info("{}: Setting Transfer 'shadowPrefix' to: {}", sessionId, shadowPrefix);
            executeSessionService.getSession(sessionId).getHmsMirrorConfig().getTransfer().setShadowPrefix(shadowPrefix);
        }
        if (exportBaseDirPrefix != null) {
            log.info("{}: Setting Transfer 'exportBaseDirPrefix' to: {}", sessionId, exportBaseDirPrefix);
            executeSessionService.getSession(sessionId).getHmsMirrorConfig().getTransfer().setExportBaseDirPrefix(exportBaseDirPrefix);
        }
        if (remoteWorkingDirectory != null) {
            log.info("{}: Setting Transfer 'remoteWorkingDirectory' to: {}", sessionId, remoteWorkingDirectory);
            executeSessionService.getSession(sessionId).getHmsMirrorConfig().getTransfer().setRemoteWorkingDirectory(remoteWorkingDirectory);
        }
        if (intermediateStorage != null) {
            log.info("{}: Setting Transfer 'intermediateStorage' to: {}", sessionId, intermediateStorage);
            executeSessionService.getSession(sessionId).getHmsMirrorConfig().getTransfer().setIntermediateStorage(intermediateStorage);
        }
        if (commonStorage != null) {
            log.info("{}: Setting Transfer 'commonStorage' to: {}", sessionId, commonStorage);
            executeSessionService.getSession(sessionId).getHmsMirrorConfig().getTransfer().setCommonStorage(commonStorage);
        }
        return executeSessionService.getSession(sessionId).getHmsMirrorConfig().getTransfer();
    }

    // Transfer / Warehouse
    @Operation(summary = "Set Warehouse Details")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Warehouse Details set",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = WarehouseConfig.class))})})
    @ResponseBody
    @RequestMapping(method = RequestMethod.PUT, value = "/transfer/warehouse")
    public WarehouseConfig setTransferWarehouse(@RequestParam(name = "sessionId", required = false) String sessionId,
            @RequestParam(value = "managedDirectory", required = false) String managedDirectory,
            @RequestParam(value = "externalDirectory", required = false) String externalDirectory
    ) {
        if (managedDirectory != null) {
            log.info("{}: Setting Warehouse 'managedDirectory' to: {}", sessionId, managedDirectory);
            executeSessionService.getSession(sessionId).getHmsMirrorConfig().getTransfer().getWarehouse().setManagedDirectory(managedDirectory);
        }
        if (externalDirectory != null) {
            log.info("{}: Setting Warehouse 'externalDirectory' to: {}", sessionId, externalDirectory);
            executeSessionService.getSession(sessionId).getHmsMirrorConfig().getTransfer().getWarehouse().setExternalDirectory(externalDirectory);
        }
        return executeSessionService.getSession(sessionId).getHmsMirrorConfig().getTransfer().getWarehouse();
    }

    // Transfer / Storage Migration
    @Operation(summary = "Set Storage Migration Details")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Storage Migration Details set",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = StorageMigration.class))})})
    @ResponseBody
    @RequestMapping(method = RequestMethod.PUT, value = "/transfer/storageMigration")
    public StorageMigration setStorageMigration(@RequestParam(name = "sessionId", required = false) String sessionId,
            @RequestParam(value = "strategy", required = false) DataStrategyEnum strategy,
            @RequestParam(value = "distcp", required = false) Boolean distcp,
            @RequestParam(value = "dataFlow", required = false) DistcpFlow dataFlow
    ) {
        if (strategy != null) {
            log.info("{}: Setting Storage Migration 'strategy' to: {}", sessionId, strategy);
            executeSessionService.getSession(sessionId).getHmsMirrorConfig().getTransfer().getStorageMigration().setStrategy(strategy);
        }
        if (distcp != null) {
            log.info("{}: Setting Storage Migration 'distcp' to: {}", sessionId, distcp);
            executeSessionService.getSession(sessionId).getHmsMirrorConfig().getTransfer().getStorageMigration().setDistcp(distcp);
        }
        if (dataFlow != null) {
            log.info("{}: Setting Storage Migration 'dataFlow' to: {}", sessionId, dataFlow);
            executeSessionService.getSession(sessionId).getHmsMirrorConfig().getTransfer().getStorageMigration().setDataFlow(dataFlow);
        }
        return executeSessionService.getSession(sessionId).getHmsMirrorConfig().getTransfer().getStorageMigration();
    }


}
