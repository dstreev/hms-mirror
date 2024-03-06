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
import com.cloudera.utils.hadoop.hms.mirror.MirrorConf;
import com.cloudera.utils.hadoop.hms.util.TableUtils;
import lombok.extern.slf4j.Slf4j;

import java.text.MessageFormat;

import static com.cloudera.utils.hadoop.hms.mirror.TablePropertyVars.EXTERNAL_TABLE_PURGE;

@Slf4j
public class ConvertLinkedDataStrategy extends DataStrategyBase implements DataStrategy {
//    private static final Logger log = LoggerFactory.getLogger(ConvertLinkedDataStrategy.class);
    @Override
    public Boolean execute() {
        Boolean rtn = Boolean.FALSE;
        EnvironmentTable let = tableMirror.getEnvironmentTable(Environment.LEFT);
        EnvironmentTable ret = tableMirror.getEnvironmentTable(Environment.RIGHT);
        try {
//        tblMirror.setResolvedDbName(config.getResolvedDB(tblMirror.getParent().getName()));

            // If RIGHT doesn't exist, run SCHEMA_ONLY.
            if (ret == null) {
                tableMirror.addIssue(Environment.RIGHT, "Table doesn't exist.  To transfer, run 'SCHEMA_ONLY'");
            } else {
                // Make sure table isn't an ACID table.
                if (TableUtils.isACID(let)) {
                    tableMirror.addIssue(Environment.LEFT, "ACID tables not eligible for this operation");
                } else if (tableMirror.isPartitioned(Environment.LEFT)) {
                    // We need to drop the RIGHT and RECREATE.
                    ret.addIssue("Table is partitioned.  Need to change data strategy to drop and recreate.");
                    String useDb = MessageFormat.format(MirrorConf.USE, tableMirror.getParent().getResolvedName());
                    ret.addSql(MirrorConf.USE_DESC, useDb);

                    // Make sure the table is NOT set to purge.
                    if (TableUtils.isExternalPurge(ret)) {
                        String purgeSql = MessageFormat.format(MirrorConf.REMOVE_TABLE_PROP, ret.getName(), EXTERNAL_TABLE_PURGE);
                        ret.addSql(MirrorConf.REMOVE_TABLE_PROP_DESC, purgeSql);
                    }

                    String dropTable = MessageFormat.format(MirrorConf.DROP_TABLE, tableMirror.getName());
                    ret.addSql(MirrorConf.DROP_TABLE_DESC, dropTable);
                    tableMirror.setStrategy(DataStrategyEnum.SCHEMA_ONLY);
                    // Set False that it doesn't exists, which it won't, since we're dropping it.
                    ret.setExists(Boolean.FALSE);
                    // Fallback to SCHEMA_ONLY.
                    DataStrategy soDS = DataStrategyEnum.SCHEMA_ONLY.getDataStrategy();
                    soDS.setTableMirror(tableMirror);
                    soDS.setDBMirror(dbMirror);
                    soDS.setConfig(config);
                    rtn = soDS.execute();
                } else {
                    // - AVRO LOCATION
                    if (AVROCheck()) {
                        String useDb = MessageFormat.format(MirrorConf.USE, tableMirror.getParent().getResolvedName());
                        ret.addSql(MirrorConf.USE_DESC, useDb);
                        // Look at the table definition and get.
                        // - LOCATION
                        String sourceLocation = TableUtils.getLocation(ret.getName(), ret.getDefinition());
                        String targetLocation = config.getTranslator().
                                translateTableLocation(tableMirror, sourceLocation, 1, null);
                        String alterLocSql = MessageFormat.format(MirrorConf.ALTER_TABLE_LOCATION, ret.getName(), targetLocation);
                        ret.addSql(MirrorConf.ALTER_TABLE_LOCATION_DESC, alterLocSql);
                        // TableUtils.updateTableLocation(ret, targetLocation)
                        // - Check Comments for "legacy.managed" setting.
                        //    - MirrorConf.HMS_MIRROR_LEGACY_MANAGED_FLAG (if so, set purge flag MirrorConf.EXTERNAL_TABLE_PURGE)
                        if (TableUtils.isHMSLegacyManaged(ret)) {
                            // ALTER TABLE x SET TBLPROPERTIES ('purge flag').
                            String purgeSql = MessageFormat.format(MirrorConf.ADD_TABLE_PROP, ret.getName(), EXTERNAL_TABLE_PURGE, "true");
                            ret.addSql(MirrorConf.ADD_TABLE_PROP_DESC, purgeSql);
                        }
                        rtn = Boolean.TRUE;

                        // Execute the RIGHT sql if config.execute.
                        if (rtn) {
                            rtn = config.getCluster(Environment.RIGHT).runTableSql(tableMirror);
                        }
                    }
                }
            }
        } catch (Throwable t) {
            log.error("Error executing ConvertLinkedDataStrategy", t);
            let.addIssue(t.getMessage());
            rtn = Boolean.FALSE;
        }

        return rtn;
    }

    @Override
    public Boolean buildOutDefinition() {
        return null;
    }

    @Override
    public Boolean buildOutSql() {
        return null;
    }
}
