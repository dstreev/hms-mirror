/*
 * Copyright (c) 2023. Cloudera, Inc. All Rights Reserved
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

package com.cloudera.utils.hadoop.hms.mirror.datastrategy;

import com.cloudera.utils.hadoop.hms.mirror.Environment;
import com.cloudera.utils.hadoop.hms.mirror.EnvironmentTable;
import com.cloudera.utils.hadoop.hms.mirror.TableMirror;
import com.cloudera.utils.hadoop.hms.mirror.service.ConfigService;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class HybridAcidDowngradeInPlaceDataStrategy extends DataStrategyBase implements DataStrategy  {

    @Getter
    private SQLAcidDowngradeInPlaceDataStrategy sqlAcidDowngradeInPlaceDataStrategy;

    @Getter
    private ExportImportAcidDowngradeInPlaceDataStrategy exportImportAcidDowngradeInPlaceDataStrategy;

    @Autowired
    public void setSqlAcidDowngradeInPlaceDataStrategy(SQLAcidDowngradeInPlaceDataStrategy sqlAcidDowngradeInPlaceDataStrategy) {
        this.sqlAcidDowngradeInPlaceDataStrategy = sqlAcidDowngradeInPlaceDataStrategy;
    }

    @Autowired
    public void setExportImportAcidDowngradeInPlaceDataStrategy(ExportImportAcidDowngradeInPlaceDataStrategy exportImportAcidDowngradeInPlaceDataStrategy) {
        this.exportImportAcidDowngradeInPlaceDataStrategy = exportImportAcidDowngradeInPlaceDataStrategy;
    }

    public HybridAcidDowngradeInPlaceDataStrategy(ConfigService configService) {
        this.configService = configService;
    }

    @Override
    public Boolean execute(TableMirror tableMirror) {
        Boolean rtn = Boolean.TRUE;
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
        if (getConfigService().getConfig().getCluster(Environment.LEFT).isLegacyHive()) {
//            DataStrategy dsSADI = DataStrategyEnum.SQL_ACID_DOWNGRADE_INPLACE.getDataStrategy();
//            dsSADI.setTableMirror(tableMirror);
//            dsSADI.setDBMirror(dbMirror);
//            dsSADI.setConfig(config);
//            rtn = dsSADI.execute();// doSQLACIDDowngradeInplace();
            rtn = sqlAcidDowngradeInPlaceDataStrategy.execute(tableMirror);
        } else {
            EnvironmentTable let = tableMirror.getEnvironmentTable(Environment.LEFT);
            if (let.getPartitioned()) {
                // Partitions less than export limit or export limit set to 0 (or less), which means ignore.
                if (let.getPartitions().size() < getConfigService().getConfig().getHybrid().getExportImportPartitionLimit() ||
                        getConfigService().getConfig().getHybrid().getExportImportPartitionLimit() <= 0) {
//                    DataStrategy dsEI = DataStrategyEnum.EXPORT_IMPORT_ACID_DOWNGRADE_INPLACE.getDataStrategy();
//                    dsEI.setTableMirror(tableMirror);
//                    dsEI.setDBMirror(dbMirror);
//                    dsEI.setConfig(config);
//                    rtn = dsEI.execute();// doEXPORTIMPORTACIDInplaceDowngrade();
                    rtn = exportImportAcidDowngradeInPlaceDataStrategy.execute(tableMirror);
                } else {
//                    DataStrategy dsSADI = DataStrategyEnum.SQL_ACID_DOWNGRADE_INPLACE.getDataStrategy();
//                    dsSADI.setTableMirror(tableMirror);
//                    dsSADI.setDBMirror(dbMirror);
//                    dsSADI.setConfig(config);
//                    rtn = dsSADI.execute();// doSQLACIDDowngradeInplace();
                    rtn = sqlAcidDowngradeInPlaceDataStrategy.execute(tableMirror);
                }
            } else {
                // Go with EXPORT_IMPORT
//                DataStrategy dsEI = DataStrategyEnum.EXPORT_IMPORT_ACID_DOWNGRADE_INPLACE.getDataStrategy();
//                dsEI.setTableMirror(tableMirror);
//                dsEI.setDBMirror(dbMirror);
//                dsEI.setConfig(config);
//                rtn = dsEI.execute();// doEXPORTIMPORTACIDInplaceDowngrade();
                rtn = exportImportAcidDowngradeInPlaceDataStrategy.execute(tableMirror);
            }
        }
        return rtn;
    }

    @Override
    public Boolean buildOutDefinition(TableMirror tableMirror) {
        return null;
    }

    @Override
    public Boolean buildOutSql(TableMirror tableMirror) {
        return null;
    }
}
