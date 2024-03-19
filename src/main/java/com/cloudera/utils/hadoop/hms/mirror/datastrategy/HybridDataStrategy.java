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

import com.cloudera.utils.hadoop.hms.mirror.Config;
import com.cloudera.utils.hadoop.hms.mirror.Environment;
import com.cloudera.utils.hadoop.hms.mirror.EnvironmentTable;
import com.cloudera.utils.hadoop.hms.mirror.TableMirror;
import com.cloudera.utils.hadoop.hms.mirror.service.ConfigService;
import com.cloudera.utils.hadoop.hms.util.TableUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class HybridDataStrategy extends DataStrategyBase implements DataStrategy {

    @Getter
    private IntermediateDataStrategy intermediateDataStrategy;

    @Getter
    private SQLDataStrategy sqlDataStrategy;

    @Getter
    private ExportImportDataStrategy exportImportDataStrategy;

    @Autowired
    public void setIntermediateDataStrategy(IntermediateDataStrategy intermediateDataStrategy) {
        this.intermediateDataStrategy = intermediateDataStrategy;
    }

    @Autowired
    public void setSqlDataStrategy(SQLDataStrategy sqlDataStrategy) {
        this.sqlDataStrategy = sqlDataStrategy;
    }

    @Autowired
    public void setExportImportDataStrategy(ExportImportDataStrategy exportImportDataStrategy) {
        this.exportImportDataStrategy = exportImportDataStrategy;
    }

    public HybridDataStrategy(ConfigService configService) {
        this.configService = configService;
    }

    @Override
    public Boolean execute(TableMirror tableMirror) {
        Boolean rtn = Boolean.FALSE;
        Config config = getConfigService().getConfig();


        // Need to look at table.  ACID tables go to doACID()
        EnvironmentTable let = tableMirror.getEnvironmentTable(Environment.LEFT);

        // Acid tables between legacy and non-legacy are forced to intermediate
        if (TableUtils.isACID(let) && getConfigService().legacyMigration()) {
            tableMirror.setStrategy(DataStrategyEnum.ACID);
            if (getConfigService().getConfig().getMigrateACID().isOn()) {
//                DataStrategy dsIt = DataStrategyEnum.INTERMEDIATE.getDataStrategy();
//                dsIt.setTableMirror(tableMirror);
//                dsIt.setDBMirror(dbMirror);
//                dsIt.setConfig(config);
//                rtn = dsIt.execute();//doIntermediateTransfer();
                rtn = intermediateDataStrategy.execute(tableMirror);
            } else {
                let.addIssue(TableUtils.ACID_NOT_ON);
                rtn = Boolean.FALSE;
            }
        } else {
            if (let.getPartitioned()) {
                if (let.getPartitions().size() > config.getHybrid().getExportImportPartitionLimit() &&
                        config.getHybrid().getExportImportPartitionLimit() > 0) {
                    // SQL
                    let.addIssue("The number of partitions: " + let.getPartitions().size()
                            + " exceeds the EXPORT_IMPORT "
                            + "partition limit (hybrid->exportImportPartitionLimit) of "
                            + config.getHybrid().getExportImportPartitionLimit() +
                            ".  Hence, the SQL method has been selected for the migration.");

                    tableMirror.setStrategy(DataStrategyEnum.SQL);
                    if (getConfigService().getConfig().getTransfer().getIntermediateStorage() != null
                            || getConfigService().getConfig().getTransfer().getCommonStorage() != null) {
//                        DataStrategy dsIt = DataStrategyEnum.INTERMEDIATE.getDataStrategy();
//                        dsIt.setTableMirror(tableMirror);
//                        dsIt.setDBMirror(dbMirror);
//                        dsIt.setConfig(config);
//                        rtn = dsIt.execute();//doIntermediateTransfer();
                        rtn = intermediateDataStrategy.execute(tableMirror);
                    } else {
//                        DataStrategy dsSQL = DataStrategyEnum.SQL.getDataStrategy();
//                        dsSQL.setTableMirror(tableMirror);
//                        dsSQL.setDBMirror(dbMirror);
//                        dsSQL.setConfig(config);
//                        rtn = dsSQL.execute();// doSQL();
                        rtn = sqlDataStrategy.execute(tableMirror);
                    }
                } else {
                    // EXPORT
                    tableMirror.setStrategy(DataStrategyEnum.EXPORT_IMPORT);
//                    DataStrategy dsEI = DataStrategyEnum.EXPORT_IMPORT.getDataStrategy();
//                    dsEI.setTableMirror(tableMirror);
//                    dsEI.setDBMirror(dbMirror);
//                    dsEI.setConfig(config);
//                    rtn = dsEI.execute();// doExportImport();
                    rtn = exportImportDataStrategy.execute(tableMirror);
                }
            } else {
                // EXPORT
                tableMirror.setStrategy(DataStrategyEnum.EXPORT_IMPORT);
//                DataStrategy dsEI = DataStrategyEnum.EXPORT_IMPORT.getDataStrategy();
//                dsEI.setTableMirror(tableMirror);
//                dsEI.setDBMirror(dbMirror);
//                dsEI.setConfig(config);
//                rtn = dsEI.execute();// doExportImport();
                rtn = exportImportDataStrategy.execute(tableMirror);
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
