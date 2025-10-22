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

import com.cloudera.utils.hms.mirror.domain.core.DBMirror;
import com.cloudera.utils.hms.mirror.domain.core.EnvironmentTable;
import com.cloudera.utils.hms.mirror.domain.core.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.core.TableMirror;
import com.cloudera.utils.hms.mirror.domain.dto.ConfigLiteDto;
import com.cloudera.utils.hms.mirror.domain.dto.JobDto;
import com.cloudera.utils.hms.mirror.domain.support.ConversionResult;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.exceptions.MissingDataPointException;
import com.cloudera.utils.hms.mirror.service.ExecuteSessionService;
import com.cloudera.utils.hms.mirror.service.StatsCalculatorService;
import com.cloudera.utils.hms.mirror.service.TranslatorService;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@Getter
public class HybridAcidDowngradeInPlaceDataStrategy extends DataStrategyBase {

    private final SQLAcidInPlaceDataStrategy sqlAcidInPlaceDataStrategy;

    private final ExportImportAcidDowngradeInPlaceDataStrategy exportImportAcidDowngradeInPlaceDataStrategy;

    public HybridAcidDowngradeInPlaceDataStrategy(StatsCalculatorService statsCalculatorService,
                                                  ExecuteSessionService executeSessionService,
                                                  TranslatorService translatorService,
                                                  SQLAcidInPlaceDataStrategy sqlAcidInPlaceDataStrategy,
                                                  ExportImportAcidDowngradeInPlaceDataStrategy exportImportAcidDowngradeInPlaceDataStrategy) {
        super(statsCalculatorService, executeSessionService, translatorService);
        this.sqlAcidInPlaceDataStrategy = sqlAcidInPlaceDataStrategy;
        this.exportImportAcidDowngradeInPlaceDataStrategy = exportImportAcidDowngradeInPlaceDataStrategy;
    }

    @Override
    public Boolean buildOutDefinition(ConversionResult conversionResult, DBMirror dbMirror, TableMirror tableMirror) {
        return null;
    }

    @Override
    public Boolean buildOutSql(ConversionResult conversionResult, DBMirror dbMirror, TableMirror tableMirror) throws MissingDataPointException {
        return null;
    }

    @Override
    public Boolean build(ConversionResult conversionResult, DBMirror dbMirror, TableMirror tableMirror) {
        Boolean rtn = Boolean.TRUE;
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
            rtn = sqlAcidInPlaceDataStrategy.build(conversionResult, dbMirror, tableMirror);
        } else {
            EnvironmentTable let = tableMirror.getEnvironmentTable(Environment.LEFT);
            if (let.getPartitioned()) {
                // Partitions less than export limit or export limit set to 0 (or less), which means ignore.
                if (let.getPartitions().size() < job.getHybrid().getExportImportPartitionLimit() ||
                        job.getHybrid().getExportImportPartitionLimit() <= 0) {
                    rtn = exportImportAcidDowngradeInPlaceDataStrategy.build(conversionResult, dbMirror, tableMirror);
                } else {
                    rtn = sqlAcidInPlaceDataStrategy.build(conversionResult, dbMirror, tableMirror);
                }
            } else {
                // Go with EXPORT_IMPORT
                rtn = exportImportAcidDowngradeInPlaceDataStrategy.build(conversionResult, dbMirror, tableMirror);
            }
        }
        return rtn;
    }

    @Override
    public Boolean execute(ConversionResult conversionResult, DBMirror dbMirror, TableMirror tableMirror) {
        Boolean rtn = Boolean.TRUE;
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();
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
        if (hmsMirrorConfig.getCluster(Environment.LEFT).isLegacyHive()) {
            rtn = sqlAcidInPlaceDataStrategy.execute(conversionResult, dbMirror, tableMirror);
        } else {
            EnvironmentTable let = tableMirror.getEnvironmentTable(Environment.LEFT);
            if (let.getPartitioned()) {
                // Partitions less than export limit or export limit set to 0 (or less), which means ignore.
                if (let.getPartitions().size() < hmsMirrorConfig.getHybrid().getExportImportPartitionLimit() ||
                        hmsMirrorConfig.getHybrid().getExportImportPartitionLimit() <= 0) {
                    rtn = exportImportAcidDowngradeInPlaceDataStrategy.execute(conversionResult, dbMirror, tableMirror);
                } else {
                    rtn = sqlAcidInPlaceDataStrategy.execute(conversionResult, dbMirror, tableMirror);
                }
            } else {
                // Go with EXPORT_IMPORT
                rtn = exportImportAcidDowngradeInPlaceDataStrategy.execute(conversionResult, dbMirror, tableMirror);
            }
        }
        return rtn;
    }

}
