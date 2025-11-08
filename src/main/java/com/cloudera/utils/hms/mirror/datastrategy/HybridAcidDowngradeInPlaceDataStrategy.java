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
import com.cloudera.utils.hms.mirror.domain.dto.JobDto;
import com.cloudera.utils.hms.mirror.domain.support.ConversionResult;
import com.cloudera.utils.hms.mirror.domain.support.DataStrategyEnum;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.exceptions.MissingDataPointException;
import com.cloudera.utils.hms.mirror.service.*;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@Getter
public class HybridAcidDowngradeInPlaceDataStrategy extends DataStrategyBase {

    private final SQLAcidInPlaceDataStrategy sqlAcidInPlaceDataStrategy;
    private final ExportImportAcidDowngradeInPlaceDataStrategy exportImportAcidDowngradeInPlaceDataStrategy;

    public HybridAcidDowngradeInPlaceDataStrategy(@NonNull ConversionResultService conversionResultService,
                                                  @NonNull ExecutionContextService executionContextService,
                                                  @NonNull StatsCalculatorService statsCalculatorService,
                                                  @NonNull CliEnvironment cliEnvironment,
                                                  @NonNull TranslatorService translatorService,
                                                  @NonNull FeatureService featureService,
                                                  SQLAcidInPlaceDataStrategy sqlAcidInPlaceDataStrategy,
                                                  ExportImportAcidDowngradeInPlaceDataStrategy exportImportAcidDowngradeInPlaceDataStrategy) {
        super(conversionResultService, executionContextService, statsCalculatorService, cliEnvironment, translatorService, featureService);
        this.sqlAcidInPlaceDataStrategy = sqlAcidInPlaceDataStrategy;
        this.exportImportAcidDowngradeInPlaceDataStrategy = exportImportAcidDowngradeInPlaceDataStrategy;
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
        Boolean rtn = Boolean.TRUE;
        ConversionResult conversionResult = getExecutionContextService().getConversionResult().orElseThrow(() ->
                new IllegalStateException("No ConversionResult found in the execution context."));
        JobDto job = conversionResult.getJob();

        if (tableMirror.getStrategy() == null) {
            tableMirror.setStrategy(DataStrategyEnum.HYBRID_ACID_DOWNGRADE_INPLACE);
        }

        /*
        Check environment is Hive 3.
            if not, need to do SQLACIDInplaceDowngrade.
        If table is not partitioned
            go to export import downgrade inplace
        else if partitions <= hybrid.exportImportPartitionLimit
            go to export import downgrade inplace
        else if partitions <= hybrid.sqlPartitionLimit
            go to sql downgrade inplace
        else
            too many partitions.
         */
        if (conversionResult.getConnection(Environment.LEFT).getPlatformType().isLegacyHive()) {
            rtn = sqlAcidInPlaceDataStrategy.build(dbMirror, tableMirror);
        } else {
            EnvironmentTable let = tableMirror.getEnvironmentTable(Environment.LEFT);
            if (let.getPartitioned()) {
                // Partitions less than export limit or export limit set to 0 (or less), which means ignore.
                if (let.getPartitions().size() < job.getHybrid().getExportImportPartitionLimit() ||
                        job.getHybrid().getExportImportPartitionLimit() <= 0) {
                    rtn = exportImportAcidDowngradeInPlaceDataStrategy.build(dbMirror, tableMirror);
                } else {
                    rtn = sqlAcidInPlaceDataStrategy.build(dbMirror, tableMirror);
                }
            } else {
                // Go with EXPORT_IMPORT
                rtn = exportImportAcidDowngradeInPlaceDataStrategy.build(dbMirror, tableMirror);
            }
        }
        return rtn;
    }

    @Override
    public Boolean execute(DBMirror dbMirror, TableMirror tableMirror) {
        Boolean rtn = Boolean.TRUE;
        ConversionResult conversionResult = getExecutionContextService().getConversionResult().orElseThrow(() ->
                new IllegalStateException("No ConversionResult found in the execution context."));
        JobDto job = conversionResult.getJob();
        /*
        Check environment is Hive 3.
            if not, need to do SQLACIDInplaceDowngrade.
        If table is not partitioned
            go to export import downgrade inplace
        else if partitions <= hybrid.exportImportPartitionLimit
            go to export import downgrade inplace
        else if partitions <= hybrid.sqlPartitionLimit
            go to sql downgrade inplace
        else
            too many partitions.
         */
        if (conversionResult.getConnection(Environment.LEFT).getPlatformType().isLegacyHive()) {
            rtn = sqlAcidInPlaceDataStrategy.execute(dbMirror, tableMirror);
        } else {
            EnvironmentTable let = tableMirror.getEnvironmentTable(Environment.LEFT);
            if (let.getPartitioned()) {
                // Partitions less than export limit or export limit set to 0 (or less), which means ignore.
                if (let.getPartitions().size() < job.getHybrid().getExportImportPartitionLimit() ||
                        job.getHybrid().getExportImportPartitionLimit() <= 0) {
                    rtn = exportImportAcidDowngradeInPlaceDataStrategy.execute(dbMirror, tableMirror);
                } else {
                    rtn = sqlAcidInPlaceDataStrategy.execute(dbMirror, tableMirror);
                }
            } else {
                // Go with EXPORT_IMPORT
                rtn = exportImportAcidDowngradeInPlaceDataStrategy.execute(dbMirror, tableMirror);
            }
        }
        return rtn;
    }

}
