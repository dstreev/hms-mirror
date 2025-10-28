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
import com.cloudera.utils.hms.mirror.CreateStrategy;
import com.cloudera.utils.hms.mirror.domain.core.DBMirror;
import com.cloudera.utils.hms.mirror.domain.core.EnvironmentTable;
import com.cloudera.utils.hms.mirror.domain.core.TableMirror;
import com.cloudera.utils.hms.mirror.domain.dto.JobDto;
import com.cloudera.utils.hms.mirror.domain.support.ConversionResult;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.exceptions.MissingDataPointException;
import com.cloudera.utils.hms.mirror.exceptions.RequiredConfigurationException;
import com.cloudera.utils.hms.mirror.service.*;
import com.cloudera.utils.hms.util.TableUtils;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.text.MessageFormat;

import static com.cloudera.utils.hms.mirror.MessageCode.*;

@Component
@Slf4j
@Getter
public class LinkedDataStrategy extends DataStrategyBase {

    private final SchemaOnlyDataStrategy schemaOnlyDataStrategy;
    private final TableService tableService;

    public LinkedDataStrategy(@NonNull ConversionResultService conversionResultService,
                              @NonNull ExecutionContextService executionContextService,
                              @NonNull StatsCalculatorService statsCalculatorService, @NonNull CliEnvironment cliEnvironment,
                              @NonNull TranslatorService translatorService, @NonNull FeatureService featureService,
                              SchemaOnlyDataStrategy schemaOnlyDataStrategy, TableService tableService) {
        super(conversionResultService, executionContextService, statsCalculatorService, cliEnvironment, translatorService, featureService);
        this.schemaOnlyDataStrategy = schemaOnlyDataStrategy;
        this.tableService = tableService;
    }

    @Override
    public Boolean buildOutDefinition(DBMirror dbMirror, TableMirror tableMirror) throws RequiredConfigurationException {
        Boolean rtn = Boolean.FALSE;
        ConversionResult conversionResult = getExecutionContextService().getConversionResult().orElseThrow(() ->
                new IllegalStateException("No ConversionResult found in the execution context."));
        JobDto job = conversionResult.getJob();

        log.debug("Table: {} buildout LINKED Definition", tableMirror.getName());
        EnvironmentTable let = null;
        EnvironmentTable ret = null;
        CopySpec copySpec = null;

        let = tableMirror.getEnvironmentTable(Environment.LEFT);
        ret = tableMirror.getEnvironmentTable(Environment.RIGHT);

        copySpec = new CopySpec(tableMirror, Environment.LEFT, Environment.RIGHT);
        // Can't LINK ACID tables.
        if (let.isExists()) {
            if (TableUtils.isHiveNative(let) && !TableUtils.isACID(let)) {
                // Swap out the namespace of the LEFT with the RIGHT.
                copySpec.setReplaceLocation(Boolean.FALSE);
                if (getConversionResultService().convertManaged())
                    copySpec.setUpgrade(Boolean.TRUE);
                // LINKED doesn't own the data.
                copySpec.setTakeOwnership(Boolean.FALSE);

                if (job.isSync()) {
                    // We assume that the 'definitions' are only there is the
                    //     table exists.
                    if (!let.isExists() && ret.isExists()) {
                        // If left is empty and right is not, DROP RIGHT.
                        ret.addIssue("Schema doesn't exist in 'source'.  Will be DROPPED.");
                        ret.setCreateStrategy(CreateStrategy.DROP);
                    } else if (let.isExists() && !ret.isExists()) {
                        // If left is defined and right is not, CREATE RIGHT.
                        ret.addIssue("Schema missing, will be CREATED");
                        ret.setCreateStrategy(CreateStrategy.CREATE);
                    } else if (let.isExists() && ret.isExists()) {
                        // If left and right, check schema change and replace if necessary.
                        // Compare Schemas.
                        if (tableMirror.schemasEqual(Environment.LEFT, Environment.RIGHT)) {
                            ret.addIssue(SCHEMA_EXISTS_NO_ACTION.getDesc());
                            ret.addSql(SKIPPED.getDesc(), "-- " + SCHEMA_EXISTS_NO_ACTION.getDesc());
                            ret.setCreateStrategy(CreateStrategy.LEAVE);
                            String msg = MessageFormat.format(TABLE_ISSUE.getDesc(), dbMirror.getName(), tableMirror.getName(),
                                    SCHEMA_EXISTS_NO_ACTION.getDesc());
                            log.error(msg);
                        } else {
                            if (TableUtils.isExternalPurge(ret)) {
                                ret.addError("Schema exists AND DOESN'T match.  But the 'RIGHT' table is has a PURGE option set. " +
                                        "We can NOT safely replace the table without compromising the data. No action will be taken.");
                                ret.setCreateStrategy(CreateStrategy.LEAVE);
                                return Boolean.FALSE;
                            } else {
                                ret.addIssue("Schema exists AND DOESN'T match.  It will be REPLACED (DROPPED and RECREATED).");
                                ret.setCreateStrategy(CreateStrategy.REPLACE);
                            }
                        }
                    }
                    copySpec.setTakeOwnership(Boolean.FALSE);
                } else {
                    if (ret.isExists()) {
                        // Already exists, no action.
                        ret.addError("Schema exists already, no action. If you wish to rebuild the schema, " +
                                "drop it first and try again. <b>Any following messages MAY be irrelevant about schema adjustments.</b>");
                        ret.setCreateStrategy(CreateStrategy.LEAVE);
                        return Boolean.FALSE;
                    } else {
                        ret.addError("Schema will be created");
                        ret.setCreateStrategy(CreateStrategy.CREATE);
                    }
                }
                // Rebuild Target from Source.
                rtn = buildTableSchema(copySpec, dbMirror);
            } else {
                let.addError("Can't LINK ACID tables");
                ret.setCreateStrategy(CreateStrategy.NOTHING);
            }
        } else {
            let.addIssue(SCHEMA_EXISTS_TARGET_MISMATCH.getDesc());
            ret.setCreateStrategy(CreateStrategy.NOTHING);
            rtn = Boolean.TRUE;
        }
        return rtn;
    }

    @Override
    public Boolean buildOutSql(DBMirror dbMirror, TableMirror tableMirror) throws MissingDataPointException {
        // Reuse the SchemaOnlyDataStrategy to build out the DDL SQL for the LINKED table.
        return schemaOnlyDataStrategy.buildOutSql(dbMirror, tableMirror);
    }

    @Override
    public Boolean build(DBMirror dbMirror, TableMirror tableMirror) {
        Boolean rtn = Boolean.FALSE;
        EnvironmentTable let = tableMirror.getEnvironmentTable(Environment.LEFT);

        if (TableUtils.isACID(let)) {
            tableMirror.addIssue(Environment.LEFT, "You can't 'LINK' ACID tables.");
            rtn = Boolean.FALSE;
        } else {
            try {
                rtn = buildOutDefinition(dbMirror, tableMirror);//tblMirror.buildoutLINKEDDefinition(config, dbMirror);
            } catch (RequiredConfigurationException e) {
                let.addError("Failed to build out definition: " + e.getMessage());
                rtn = Boolean.FALSE;
            }
        }

        if (rtn) {
            try {
                rtn = buildOutSql(dbMirror, tableMirror);//tblMirror.buildoutLINKEDSql(config, dbMirror);
            } catch (MissingDataPointException e) {
                let.addError("Failed to build out SQL: " + e.getMessage());
                rtn = Boolean.FALSE;
            }
        }

        // Execute the RIGHT sql if config.execute.
//        if (rtn) {
//            rtn = tableService.runTableSql(tableMirror, Environment.RIGHT);
//        }

        return rtn;

    }

    @Override
    public Boolean execute(DBMirror dbMirror, TableMirror tableMirror) {
        return tableService.runTableSql(tableMirror, Environment.RIGHT);
    }

}
