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
import com.cloudera.utils.hms.mirror.Pair;
import com.cloudera.utils.hms.mirror.domain.core.DBMirror;
import com.cloudera.utils.hms.mirror.domain.core.EnvironmentTable;
import com.cloudera.utils.hms.mirror.domain.core.TableMirror;
import com.cloudera.utils.hms.mirror.domain.dto.ConfigLiteDto;
import com.cloudera.utils.hms.mirror.domain.dto.JobDto;
import com.cloudera.utils.hms.mirror.domain.support.ConversionResult;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.domain.support.RunStatus;
import com.cloudera.utils.hms.mirror.exceptions.MissingDataPointException;
import com.cloudera.utils.hms.mirror.exceptions.RequiredConfigurationException;
import com.cloudera.utils.hms.mirror.service.*;
import com.cloudera.utils.hms.util.ConfigUtils;
import com.cloudera.utils.hms.util.TableUtils;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.text.MessageFormat;
import java.util.List;

import static com.cloudera.utils.hms.mirror.MessageCode.*;
import static com.cloudera.utils.hms.mirror.SessionVars.*;
import static com.cloudera.utils.hms.mirror.TablePropertyVars.ACID_INPLACE;
import static com.cloudera.utils.hms.mirror.TablePropertyVars.TRANSLATED_TO_EXTERNAL;

@Component
@Slf4j
@Getter
public class SQLAcidInPlaceDataStrategy extends DataStrategyBase {

    private final TableService tableService;

    public SQLAcidInPlaceDataStrategy(@NonNull ConversionResultService conversionResultService,
                                      @NonNull ExecutionContextService executionContextService,
                                      @NonNull StatsCalculatorService statsCalculatorService, @NonNull CliEnvironment cliEnvironment,
                                      @NonNull TranslatorService translatorService, @NonNull FeatureService featureService,
                                      TableService tableService) {
        super(conversionResultService, executionContextService, statsCalculatorService, cliEnvironment, translatorService, featureService);
        this.tableService = tableService;
    }

    @Override
    public Boolean buildOutDefinition(DBMirror dbMirror, TableMirror tableMirror) throws RequiredConfigurationException {
        Boolean rtn = Boolean.FALSE;
        ConversionResult conversionResult = getExecutionContextService().getConversionResult().orElseThrow(() ->
                new IllegalStateException("No ConversionResult found in the execution context."));
        ConfigLiteDto config = conversionResult.getConfig();
        JobDto job = conversionResult.getJob();
        RunStatus runStatus = conversionResult.getRunStatus();

        EnvironmentTable let = tableMirror.getEnvironmentTable(Environment.LEFT);
        EnvironmentTable ret = tableMirror.getEnvironmentTable(Environment.RIGHT);

        // Check the Left to ensure it hasn't already been processed by 'inplace'
        if (TableUtils.hasTblProperty(ACID_INPLACE, let)) {
            let.addIssue(INPLACE_MIGRATED.getDesc());
            String msg = MessageFormat.format(TABLE_ISSUE.getDesc(), dbMirror.getName(), tableMirror.getName(),
                    INPLACE_MIGRATED.getDesc());
            log.error(msg);
            return Boolean.FALSE;
        }

        // If 'inplace' and not 'downgrade', this process is about 'removing' the bucket definitions.
        // So if there are buckets and the threshold is met, skip the conversion.
        if (config.getMigrateACID().isInplace() && !config.getMigrateACID().isDowngrade()) {
            int bucketCount = TableUtils.numOfBuckets(let);
            if (bucketCount > config.getMigrateACID().getArtificialBucketThreshold()) {
                // Skip.
                String msg = MessageFormat.format(BUCKET_CONVERSION_THRESHOLD.getDesc(),
                        dbMirror.getName(), tableMirror.getName(),
                        config.getMigrateACID().getArtificialBucketThreshold(), bucketCount);
                log.error(msg);
                let.addIssue(msg);
                return Boolean.FALSE;
            }
        }

        // Use db
        String useDb = MessageFormat.format(MirrorConf.USE, dbMirror.getName());
        let.addSql(TableUtils.USE_DESC, useDb);

        // Build Right (to be used as new table on left).
        CopySpec leftNewTableSpec = new CopySpec(tableMirror, Environment.LEFT, Environment.RIGHT);
        leftNewTableSpec.setTakeOwnership(Boolean.TRUE);
        if (config.getMigrateACID().isDowngrade()) {
            leftNewTableSpec.setMakeExternal(Boolean.TRUE);
        }
        // Location of converted data will got to default location.
        leftNewTableSpec.setStripLocation(Boolean.TRUE);

        rtn = buildTableSchema(leftNewTableSpec, dbMirror);

        String origTableName = let.getName();

        // Rename Original Table
        // Remove property (if exists) to prevent rename from happening.
        if (TableUtils.hasTblProperty(TRANSLATED_TO_EXTERNAL, let)) {
            String unSetSql = MessageFormat.format(MirrorConf.REMOVE_TABLE_PROP, origTableName, TRANSLATED_TO_EXTERNAL);
            let.addSql(MirrorConf.REMOVE_TABLE_PROP_DESC, unSetSql);
        }

        String newTblName = let.getName() + "_archive";
        String renameSql = MessageFormat.format(MirrorConf.RENAME_TABLE, origTableName, newTblName);
        TableUtils.changeTableName(let, newTblName);
        let.addSql(TableUtils.RENAME_TABLE, renameSql);

        // Check Buckets and Strip.
        int buckets = TableUtils.numOfBuckets(ret);
        if (buckets > 0 && buckets <= config.getMigrateACID().getArtificialBucketThreshold()) {
            // Strip bucket definition.
            if (TableUtils.removeBuckets(ret, config.getMigrateACID().getArtificialBucketThreshold())) {
                let.addIssue("Bucket Definition removed (was " + TableUtils.numOfBuckets(ret) + ") because it was EQUAL TO or BELOW " +
                        "the configured 'artificialBucketThreshold' of " +
                        config.getMigrateACID().getArtificialBucketThreshold());
            }

        }
        // Create New Table.
        String newCreateTable = getTableService().getCreateStatement(tableMirror, Environment.RIGHT);
        let.addSql(TableUtils.CREATE_DESC, newCreateTable);

        rtn = Boolean.TRUE;

        return rtn;
    }

    @Override
    public Boolean buildOutSql(DBMirror dbMirror, TableMirror tableMirror) throws MissingDataPointException {
        Boolean rtn = Boolean.FALSE;
        ConversionResult conversionResult = getExecutionContextService().getConversionResult().orElseThrow(() ->
                new IllegalStateException("No ConversionResult found in the execution context."));
        ConfigLiteDto config = conversionResult.getConfig();
        JobDto job = conversionResult.getJob();
        RunStatus runStatus = conversionResult.getRunStatus();


        // Check to see if there are partitions.
        EnvironmentTable let = tableMirror.getEnvironmentTable(Environment.LEFT);
        EnvironmentTable ret = tableMirror.getEnvironmentTable(Environment.RIGHT);

        // Ensure we're in the right database.
        String database = dbMirror.getName();
        String useDb = MessageFormat.format(MirrorConf.USE, database);

        let.addSql(TableUtils.USE_DESC, useDb);
        // Set Override Properties.
        // Get the LEFT overrides for the DOWNGRADE.
        List<String> overrides = ConfigUtils.getPropertyOverridesFor(Environment.LEFT, config.getOptimization().getOverrides());
        for (String setCmd : overrides) {
            let.addSql(setCmd, setCmd);
        }

        if (let.getPartitioned()) {
            if (config.getOptimization().isSkip()) {
                if (!conversionResult.getConnection(Environment.LEFT).getPlatformType().isLegacyHive()) {
                    let.addSql("Setting " + SORT_DYNAMIC_PARTITION, MessageFormat.format(SET_SESSION_VALUE_STRING, SORT_DYNAMIC_PARTITION, "false"));
                }
                String partElement = TableUtils.getPartitionElements(let);
                String transferSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_WITH_PARTITIONS_DECLARATIVE,
                        let.getName(), ret.getName(), partElement);
                String transferDesc = MessageFormat.format(TableUtils.STAGE_TRANSFER_PARTITION_DESC, let.getPartitions().size());
                let.addSql(new Pair(transferDesc, transferSql));
            } else if (config.getOptimization().isSortDynamicPartitionInserts()) {
                if (!conversionResult.getConnection(Environment.LEFT).getPlatformType().isLegacyHive()) {
                    let.addSql("Setting " + SORT_DYNAMIC_PARTITION, MessageFormat.format(SET_SESSION_VALUE_STRING, SORT_DYNAMIC_PARTITION, "true"));
                    if (!conversionResult.getConnection(Environment.LEFT).getPlatformType().isHdpHive3()) {
                        let.addSql("Setting " + SORT_DYNAMIC_PARTITION_THRESHOLD, MessageFormat.format(SET_SESSION_VALUE_INT, SORT_DYNAMIC_PARTITION_THRESHOLD, 0));
                    }
                }
                String partElement = TableUtils.getPartitionElements(let);
                String transferSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_WITH_PARTITIONS_DECLARATIVE,
                        let.getName(), ret.getName(), partElement);
                String transferDesc = MessageFormat.format(TableUtils.STORAGE_MIGRATION_TRANSFER_DESC, let.getPartitions().size());
                let.addSql(new Pair(transferDesc, transferSql));
            } else {
                // Prescriptive Optimization.
                if (!conversionResult.getConnection(Environment.LEFT).getPlatformType().isLegacyHive()) {
                    let.addSql("Setting " + SORT_DYNAMIC_PARTITION, MessageFormat.format(SET_SESSION_VALUE_STRING, SORT_DYNAMIC_PARTITION, "false"));
                    if (!conversionResult.getConnection(Environment.LEFT).getPlatformType().isHdpHive3()) {
                        let.addSql("Setting " + SORT_DYNAMIC_PARTITION_THRESHOLD, MessageFormat.format(SET_SESSION_VALUE_INT, SORT_DYNAMIC_PARTITION_THRESHOLD, -1));
                    }
                }

                String partElement = TableUtils.getPartitionElements(let);
                String distPartElement = getStatsCalculatorService().getDistributedPartitionElements(let);
                String transferSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_WITH_PARTITIONS_PRESCRIPTIVE,
                        let.getName(), ret.getName(), partElement, distPartElement);
                String transferDesc = MessageFormat.format(TableUtils.STORAGE_MIGRATION_TRANSFER_DESC, let.getPartitions().size());
                let.addSql(new Pair(transferDesc, transferSql));
            }
        } else {
            // Simple FROM .. INSERT OVERWRITE ... SELECT *;
            String transferSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_OVERWRITE, let.getName(), ret.getName());
            let.addSql(new Pair(TableUtils.STORAGE_MIGRATION_TRANSFER_DESC, transferSql));
        }

        rtn = Boolean.TRUE;

        return rtn;
    }

    @Override
    public Boolean build(DBMirror dbMirror, TableMirror tableMirror) {
        Boolean rtn = Boolean.TRUE;
        ConversionResult conversionResult = getExecutionContextService().getConversionResult().orElseThrow(() ->
                new IllegalStateException("No ConversionResult found in the execution context."));
        ConfigLiteDto config = conversionResult.getConfig();
        JobDto job = conversionResult.getJob();
        RunStatus runStatus = conversionResult.getRunStatus();

        // Build cleanup Queries (drop archive table)
        EnvironmentTable let = tableMirror.getEnvironmentTable(Environment.LEFT);

        /*
        In this case, the LEFT is the source and we'll us the RIGHT cluster definition to hold the work. We need to ensure
        the RIGHT cluster is configured the same as the LEFT.
         */

        /*
        rename original table
        remove artificial bucket in new table def
        create new external table with original name
        from original_archive insert overwrite table new external (deal with partitions).
        write cleanup sql to drop original_archive.
         */
        try {
            rtn = buildOutDefinition(dbMirror, tableMirror);//tableMirror.buildoutSQLACIDDowngradeInplaceDefinition(config, dbMirror);
        } catch (RequiredConfigurationException e) {
            let.addError("Failed to build out definition: " + e.getMessage());
            rtn = Boolean.FALSE;
        }

        if (rtn) {
            String cleanUpArchive = MessageFormat.format(MirrorConf.DROP_TABLE, let.getName());
            let.addCleanUpSql(TableUtils.DROP_DESC, cleanUpArchive);

            // Check Partition Counts.
            if (let.getPartitioned() && let.getPartitions().size() > config.getMigrateACID().getPartitionLimit()) {
                let.addError("The number of partitions: " + let.getPartitions().size() + " exceeds the ACID SQL " +
                        "partition limit (migrateACID->partitionLimit) of " + config.getMigrateACID().getPartitionLimit() +
                        ".  The queries will NOT be automatically run.");
                rtn = Boolean.FALSE;
            }
        }

        if (rtn) {
            // Build Transfer SQL
            try {
                rtn = buildOutSql(dbMirror, tableMirror);
            } catch (MissingDataPointException e) {
                let.addError("Failed to build out SQL: " + e.getMessage());
                rtn = Boolean.FALSE;
            }
        }

        return rtn;
    }

    @Override
    public Boolean execute(DBMirror dbMirror, TableMirror tableMirror) {
        return getTableService().runTableSql(tableMirror, Environment.LEFT);
    }
}
