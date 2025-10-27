/*
 * Copyright (c) 2023-2025. Cloudera, Inc. All Rights Reserved
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

package com.cloudera.utils.hms.mirror.datastrategy;

import com.cloudera.utils.hadoop.cli.CliEnvironment;
import com.cloudera.utils.hms.mirror.CopySpec;
import com.cloudera.utils.hms.mirror.MirrorConf;
import com.cloudera.utils.hms.mirror.domain.core.DBMirror;
import com.cloudera.utils.hms.mirror.domain.core.EnvironmentTable;
import com.cloudera.utils.hms.mirror.domain.core.TableMirror;
import com.cloudera.utils.hms.mirror.domain.dto.ConfigLiteDto;
import com.cloudera.utils.hms.mirror.domain.support.ConversionResult;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.exceptions.MissingDataPointException;
import com.cloudera.utils.hms.mirror.service.*;
import com.cloudera.utils.hms.util.TableUtils;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.text.MessageFormat;

import static org.apache.commons.lang3.StringUtils.isBlank;

@Component
@Slf4j
@Getter
public class DumpDataStrategy extends DataStrategyBase {

    private final TableService tableService;


    public DumpDataStrategy(@NonNull ConversionResultService conversionResultService,
                            @NonNull ExecutionContextService executionContextService,
                            @NonNull StatsCalculatorService statsCalculatorService, @NonNull CliEnvironment cliEnvironment,
                            @NonNull TranslatorService translatorService, @NonNull FeatureService featureService,
                            TableService tableService) {
        super(conversionResultService, executionContextService, statsCalculatorService, cliEnvironment, translatorService, featureService);
        this.tableService = tableService;
    }

    @Override
    public Boolean buildOutDefinition(DBMirror dbMirror, TableMirror tableMirror) {
        log.debug("Table: {} buildout DUMP Definition", tableMirror.getName());
        ConversionResult conversionResult = getExecutionContextService().getConversionResult();

//        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();

        EnvironmentTable let = null;
        EnvironmentTable ret = null;
        CopySpec copySpec = null;

        let = tableMirror.getEnvironmentTable(Environment.LEFT);
        // Standardize the LEFT def.
        // Remove DB from CREATE
        TableUtils.stripDatabase(let.getName(), let.getDefinition());

        // If not legacy, remove location from ACID tables.
        if (!conversionResult.getConnection(Environment.LEFT).getPlatformType().isLegacyHive() &&
                TableUtils.isACID(let)) {
            TableUtils.stripLocation(let.getName(), let.getDefinition());
        }
        return Boolean.TRUE;
    }

    @Override
    public Boolean buildOutSql(DBMirror dbMirror, TableMirror tableMirror) throws MissingDataPointException {
        Boolean rtn = Boolean.FALSE;
        ConversionResult conversionResult = getExecutionContextService().getConversionResult();
        ConfigLiteDto config = conversionResult.getConfigLite();

        log.debug("Table: {} buildout DUMP SQL", tableMirror.getName());

        String useDb = null;
//        String database = null;
        String createTbl = null;

        EnvironmentTable let = tableMirror.getEnvironmentTable(Environment.LEFT);
        EnvironmentTable ret = tableMirror.getEnvironmentTable(Environment.RIGHT);

        let.getSql().clear();
        String database = getConversionResultService().getResolvedDB(dbMirror.getName());
        useDb = MessageFormat.format(MirrorConf.USE, database);
        let.addSql(TableUtils.USE_DESC, useDb);

        createTbl = tableService.getCreateStatement(tableMirror, Environment.LEFT);
        let.addSql(TableUtils.CREATE_DESC, createTbl);
        if (!conversionResult.getConnection(Environment.LEFT).getPlatformType().isLegacyHive()
                && config.getOwnershipTransfer().isTable() && let.getOwner() != null) {
            String ownerSql = MessageFormat.format(MirrorConf.SET_TABLE_OWNER, let.getName(), let.getOwner());
            let.addSql(MirrorConf.SET_TABLE_OWNER_DESC, ownerSql);
        }

        // If partitioned, !ACID, repair
        if (let.getPartitioned() && !TableUtils.isACID(let)) {
            if (config.loadMetadataDetails()) {
                String tableParts = getTranslatorService().buildPartitionAddStatement(let);
                // This will be empty when there's no data and we need to handle that.
                if (!isBlank(tableParts)) {
                    String addPartSql = MessageFormat.format(MirrorConf.ALTER_TABLE_PARTITION_ADD_LOCATION, let.getName(), tableParts);
                    let.addSql(MirrorConf.ALTER_TABLE_PARTITION_ADD_LOCATION_DESC, addPartSql);
                }
            } else if (conversionResult.getConnection(Environment.LEFT).isPartitionDiscoveryInitMSCK()) {
                String msckStmt = MessageFormat.format(MirrorConf.MSCK_REPAIR_TABLE, let.getName());
                if (config.getTransfer().getStorageMigration().isDistcp()) {
                    let.addCleanUpSql(TableUtils.REPAIR_DESC, msckStmt);
                } else {
                    let.addSql(TableUtils.REPAIR_DESC, msckStmt);
                }
            }
        }

        rtn = Boolean.TRUE;

        return rtn;
    }

    @Override
    public Boolean build(DBMirror dbMirror, TableMirror tableMirror) {
        Boolean rtn = Boolean.FALSE;

        rtn = buildOutDefinition(dbMirror, tableMirror);
        if (rtn) {
            try {
                rtn = buildOutSql(dbMirror, tableMirror);
            } catch (MissingDataPointException e) {
                EnvironmentTable let = getConversionResultService().getEnvironmentTable(Environment.LEFT, tableMirror);
                log.error("Table: {} Missing Data Point: {}", let.getName(), e.getMessage());
                let.addError("Failed to build out SQL: " + e.getMessage());
                rtn = Boolean.FALSE;
            }
        }
        return rtn;
    }

    @Override
    public Boolean execute(DBMirror dbMirror, TableMirror tableMirror) {
        // No Action to perform.
        return Boolean.TRUE;
    }

}
