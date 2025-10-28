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
import com.cloudera.utils.hms.mirror.domain.core.DBMirror;
import com.cloudera.utils.hms.mirror.domain.core.EnvironmentTable;
import com.cloudera.utils.hms.mirror.domain.core.TableMirror;
import com.cloudera.utils.hms.mirror.domain.dto.ConfigLiteDto;
import com.cloudera.utils.hms.mirror.domain.dto.JobDto;
import com.cloudera.utils.hms.mirror.domain.support.ConversionResult;
import com.cloudera.utils.hms.mirror.domain.support.DataStrategyEnum;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.domain.support.RunStatus;
import com.cloudera.utils.hms.mirror.exceptions.MissingDataPointException;
import com.cloudera.utils.hms.mirror.service.*;
import com.cloudera.utils.hms.util.TableUtils;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import static org.apache.commons.lang3.StringUtils.isBlank;

@Component
@Slf4j
@Getter
public class HybridDataStrategy extends DataStrategyBase {

    private final ConfigService configService;
    private final IntermediateDataStrategy intermediateDataStrategy;
    private final SQLDataStrategy sqlDataStrategy;
    private final ExportImportDataStrategy exportImportDataStrategy;

    public HybridDataStrategy(@NonNull ConversionResultService conversionResultService,
                              @NonNull ExecutionContextService executionContextService,
                              @NonNull StatsCalculatorService statsCalculatorService, @NonNull CliEnvironment cliEnvironment,
                              @NonNull TranslatorService translatorService, @NonNull FeatureService featureService,
                              ConfigService configService, IntermediateDataStrategy intermediateDataStrategy,
                              SQLDataStrategy sqlDataStrategy, ExportImportDataStrategy exportImportDataStrategy) {
        super(conversionResultService, executionContextService, statsCalculatorService, cliEnvironment, translatorService, featureService);
        this.configService = configService;
        this.intermediateDataStrategy = intermediateDataStrategy;
        this.sqlDataStrategy = sqlDataStrategy;
        this.exportImportDataStrategy = exportImportDataStrategy;
    }

    @Override
    public Boolean buildOutDefinition(DBMirror dbMirror, TableMirror tableMirror) {
        return null;
    }

    @Override
    public Boolean buildOutSql(DBMirror dbMirror, TableMirror tableMirror) throws MissingDataPointException {
        return null;
    }

    @Override
    public Boolean build(DBMirror dbMirror, TableMirror tableMirror) {
        Boolean rtn = Boolean.FALSE;

        ConversionResult conversionResult = getExecutionContextService().getConversionResult().orElseThrow(() ->
                new IllegalStateException("No ConversionResult found in the execution context."));
        ConfigLiteDto config = conversionResult.getConfig();
        JobDto job = conversionResult.getJob();
        RunStatus runStatus = conversionResult.getRunStatus();

        // Need to look at table.  ACID tables go to doACID()
        EnvironmentTable let = tableMirror.getEnvironmentTable(Environment.LEFT);

        // Acid tables between legacy and non-legacy are forced to intermediate
        if (TableUtils.isACID(let) && getConversionResultService().legacyMigration()) {
            tableMirror.setStrategy(DataStrategyEnum.ACID);
            if (config.getMigrateACID().isOn()) {
                rtn = intermediateDataStrategy.build(dbMirror, tableMirror);
            } else {
                let.addError(TableUtils.ACID_NOT_ON);
                rtn = Boolean.FALSE;
            }
        } else {
            if (let.getPartitioned()) {
                if (let.getPartitions().size() > job.getHybrid().getExportImportPartitionLimit() &&
                        job.getHybrid().getExportImportPartitionLimit() > 0) {
                    // SQL
                    let.addIssue("The number of partitions: " + let.getPartitions().size()
                            + " exceeds the EXPORT_IMPORT "
                            + "partition limit (hybrid->exportImportPartitionLimit) of "
                            + job.getHybrid().getExportImportPartitionLimit() +
                            ".  Hence, the SQL method has been selected for the migration.");

                    tableMirror.setStrategy(DataStrategyEnum.SQL);
                    if (!isBlank(config.getTransfer().getIntermediateStorage())
                            || !isBlank(config.getTransfer().getTargetNamespace())) {
                        rtn = intermediateDataStrategy.build(dbMirror, tableMirror);
                    } else {
                        rtn = sqlDataStrategy.build(dbMirror, tableMirror);
                    }
                } else {
                    // EXPORT
                    tableMirror.setStrategy(DataStrategyEnum.EXPORT_IMPORT);
                    rtn = exportImportDataStrategy.build(dbMirror, tableMirror);
                }
            } else {
                // EXPORT
                tableMirror.setStrategy(DataStrategyEnum.EXPORT_IMPORT);
                rtn = exportImportDataStrategy.build(dbMirror, tableMirror);
            }
        }
        return rtn;

    }

    @Override
    public Boolean execute(DBMirror dbMirror, TableMirror tableMirror) {
        Boolean rtn = Boolean.FALSE;

        ConversionResult conversionResult = getExecutionContextService().getConversionResult().orElseThrow(() ->
                new IllegalStateException("No ConversionResult found in the execution context."));
        ConfigLiteDto config = conversionResult.getConfig();
        JobDto job = conversionResult.getJob();
        RunStatus runStatus = conversionResult.getRunStatus();

        EnvironmentTable let = tableMirror.getEnvironmentTable(Environment.LEFT);

        if (TableUtils.isACID(let) && getConversionResultService().legacyMigration()) {
            tableMirror.setStrategy(DataStrategyEnum.ACID);
            if (config.getMigrateACID().isOn()) {
                rtn = intermediateDataStrategy.execute(dbMirror, tableMirror);
            } else {
                rtn = Boolean.FALSE;
            }
        } else {
            if (let.getPartitioned()) {
                if (let.getPartitions().size() > job.getHybrid().getExportImportPartitionLimit() &&
                        job.getHybrid().getExportImportPartitionLimit() > 0) {
                    if (!isBlank(config.getTransfer().getIntermediateStorage())
                            || !isBlank(config.getTransfer().getTargetNamespace())) {
                        rtn = intermediateDataStrategy.execute(dbMirror, tableMirror);
                    } else {
                        rtn = sqlDataStrategy.execute(dbMirror, tableMirror);
                    }
                } else {
                    // EXPORT
                    rtn = exportImportDataStrategy.execute(dbMirror, tableMirror);
                }
            } else {
                // EXPORT
                rtn = exportImportDataStrategy.execute(dbMirror, tableMirror);
            }
        }
        return rtn;
    }

}
