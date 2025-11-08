/*
 * Copyright (c) 2024-2025. Cloudera, Inc. All Rights Reserved
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

package com.cloudera.utils.hms.mirror.integration.end_to_end.cdp_to_cdp;

import com.cloudera.utils.hms.mirror.PhaseState;
import com.cloudera.utils.hms.mirror.cli.Mirror;
import com.cloudera.utils.hms.mirror.domain.support.DataStrategyEnum;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.integration.end_to_end.E2EBaseTest;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = Mirror.class,
        args = {
                "--hms-mirror.config.data-strategy=EXPORT_IMPORT",
                "--hms-mirror.config.migrate-acid=true",
                "--hms-mirror.config.warehouse-directory=/warehouse/managed",
                "--hms-mirror.config.external-warehouse-directory=/warehouse/external",
                "--hms-mirror.config.export-partition-count=500",
                "--hms-mirror.conversion.test-filename=/test_data/assorted_tbls_01.yaml",
                "--hms-mirror.config.filename=/config/default.yaml.cdp-cdp",
                "--hms-mirror.config.output-dir=${user.home}/.hms-mirror/test-output/e2e/cdp_cdp/ei_ma_wd_ep"
        })
@Slf4j
public class Test_ei_ma_wd_ep extends E2EBaseTest {

    @Test
    public void returnCodeTest() {
        // Get Runtime Return Code.
        long rtn = getReturnCode();
        // Verify the return code.
        long check = 0L;
        assertEquals(check * -1, rtn, "Return Code Failure: " + rtn);
    }

    @Test
    public void databaseLocationTest() {
        // Validate database location on RIGHT with external warehouse directory
        validateDBLocation("assorted_test_db", Environment.RIGHT, 
                "hdfs://HOME90/warehouse/tablespace/external/hive/assorted_test_db.db");
    }

    @Test
    public void checkPhaseTest() {
        // Validate phase state for acid_01 ACID table
        validatePhase("assorted_test_db", "acid_01", PhaseState.PROCESSED);
        validatePhase("assorted_test_db", "acid_03", PhaseState.PROCESSED);
        validatePhase("assorted_test_db", "acid_02", PhaseState.PROCESSED);
        validatePhase("assorted_test_db", "ext_part_01", PhaseState.PROCESSED);
        validatePhase("assorted_test_db", "ext_part_02", PhaseState.PROCESSED);
        validatePhase("assorted_test_db", "legacy_mngd_01", PhaseState.PROCESSED);
        validatePhase("assorted_test_db", "ext_missing_01", PhaseState.PROCESSED);
    }

    @Test
    public void checkExtMissing01Issue() {
        validateTableIssue("assorted_test_db","ext_missing_01", Environment.RIGHT,
                "Schema exists on the target, but not on the source.");
    }

    @Test
    public void checkTableCount() {
        // Validate that we have 7 tables in the database
        validateTableCount("assorted_test_db", 7);
    }

    @Test
    public void checkExportImportStrategy() {
        // Validate that all tables use EXPORT_IMPORT strategy
        validateTableStrategy("assorted_test_db", "acid_01", DataStrategyEnum.EXPORT_IMPORT);
        validateTableStrategy("assorted_test_db", "acid_02", DataStrategyEnum.EXPORT_IMPORT);
        validateTableStrategy("assorted_test_db", "acid_03", DataStrategyEnum.EXPORT_IMPORT);
        validateTableStrategy("assorted_test_db", "ext_part_01", DataStrategyEnum.EXPORT_IMPORT);
        validateTableStrategy("assorted_test_db", "ext_part_02", DataStrategyEnum.EXPORT_IMPORT);
        validateTableStrategy("assorted_test_db", "legacy_mngd_01", DataStrategyEnum.EXPORT_IMPORT);
        validateTableStrategy("assorted_test_db", "ext_missing_01", DataStrategyEnum.EXPORT_IMPORT);
    }

    @Test
    public void checkACIDTableIsTransactional() {
        // Validate that acid_01 is transactional
        validateTableIsACID("assorted_test_db", "acid_01", Environment.LEFT);
        validateTableProperty("assorted_test_db", "acid_01", Environment.LEFT,
                "transactional", "true");
        
        // Validate that acid_02 is transactional
        validateTableIsACID("assorted_test_db", "acid_02", Environment.LEFT);
        validateTableProperty("assorted_test_db", "acid_02", Environment.LEFT,
                "transactional", "true");
        
        // Validate that acid_03 is transactional with partitions
        validateTableIsACID("assorted_test_db", "acid_03", Environment.LEFT);
        validateTableProperty("assorted_test_db", "acid_03", Environment.LEFT,
                "transactional", "true");
    }

    @Test
    public void checkExportPaths() {
        // Validate export paths for tables
        validateTableSqlPair("assorted_test_db", Environment.LEFT, "acid_01"
                ,"EXPORT Table", "EXPORT TABLE acid_01 TO \"hdfs://HDP50/apps/hive/warehouse/export_assorted_test_db/acid_01\"");
    }

    @Test
    public void checkImportPaths() {
        // Validate import paths for tables
        validateTableSqlPair("assorted_test_db", Environment.RIGHT, "acid_01",
                "IMPORT Table", "IMPORT TABLE acid_01 FROM \"hdfs://HDP50/apps/hive/warehouse/export_assorted_test_db/acid_01\"");
    }

    @Test
    public void checkExternalTableImport() {
        // Validate IMPORT command for external table includes EXTERNAL and LOCATION
        validateTableSqlPair("assorted_test_db", Environment.RIGHT, "ext_part_01",
                "IMPORT Table",
                "IMPORT EXTERNAL TABLE ext_part_01 FROM \"hdfs://HDP50/apps/hive/warehouse/export_assorted_test_db/ext_part_01\" LOCATION \"hdfs://HOME90/warehouse/tablespace/external/hive/assorted_test_db.db/ext_part_01\"");
    }

    @Test
    public void checkPartitionCount() {
        // Validate partition count for ext_part_01
        validatePartitionCount("assorted_test_db", "ext_part_01", Environment.LEFT, 440);
        
        // ext_part_02 has no partitions based on the test data
        validatePartitionCount("assorted_test_db", "ext_part_02", Environment.LEFT, 0);
        
        // Validate partition count for acid_03
        validatePartitionCount("assorted_test_db", "acid_03", Environment.LEFT, 200);
    }

    @Test
    public void checkTableLocations() {
        // Validate ACID table locations with managed warehouse
        validateTableLocation("assorted_test_db", "acid_01", Environment.LEFT,
                "hdfs://HDP50/warehouse/tablespace/managed/hive/assorted_test_db.db/acid_01");
        validateTableLocation("assorted_test_db", "acid_02", Environment.LEFT,
                "hdfs://HDP50/warehouse/tablespace/managed/hive/assorted_test_db.db/acid_02");
        validateTableLocation("assorted_test_db", "acid_03", Environment.LEFT,
                "hdfs://HDP50/warehouse/tablespace/managed/hive/assorted_test_db.db/acid_03");
        
        // Validate external table locations
        validateTableLocation("assorted_test_db", "ext_part_01", Environment.LEFT,
                "hdfs://HDP50/warehouse/tablespace/external/hive/assorted_test_db.db/ext_part_01");
        validateTableLocation("assorted_test_db", "ext_part_02", Environment.LEFT,
                "hdfs://HDP50/warehouse/tablespace/external/hive/assorted_test_db.db/ext_part_02");
        
        // ext_missing_01 exists only on RIGHT
        validateTableLocation("assorted_test_db", "ext_missing_01", Environment.RIGHT,
                "hdfs://HDP50/warehouse/tablespace/external/hive/assorted_test_db.db/ext_missing_01");
    }

    @Test
    public void checkBucketingForACIDTables() {
        // Validate acid_01 has 2 buckets
        validateTableBuckets("assorted_test_db", "acid_01", Environment.LEFT, 2);

        // acid_02 doesn't have buckets defined
        // Validate acid_03 has 6 buckets
        validateTableBuckets("assorted_test_db", "acid_03", Environment.LEFT, 6);
    }

    @Test
    public void checkTableProperties() {
        // Validate bucketing_version for all tables
        validateTableProperty("assorted_test_db", "acid_01", Environment.LEFT,
                "bucketing_version", "2");
        validateTableProperty("assorted_test_db", "acid_02", Environment.LEFT,
                "bucketing_version", "2");
        validateTableProperty("assorted_test_db", "acid_03", Environment.LEFT,
                "bucketing_version", "2");
        validateTableProperty("assorted_test_db", "ext_part_01", Environment.LEFT,
                "bucketing_version", "2");
        validateTableProperty("assorted_test_db", "ext_part_02", Environment.LEFT,
                "bucketing_version", "2");
    }

    @Test
    public void checkDatabaseSql() {
        // Validate database creation SQL on RIGHT
        validateDBSqlPair("assorted_test_db", Environment.RIGHT,
                "Create Database",
                "CREATE DATABASE IF NOT EXISTS assorted_test_db\n");
        validateDBSqlPair("assorted_test_db", Environment.RIGHT,
                "Alter Database Location",
                "ALTER DATABASE assorted_test_db SET LOCATION \"hdfs://HOME90/warehouse/tablespace/external/hive/assorted_test_db.db\"");
    }

    @Test
    public void checkTableExistence() {
        // Validate that most tables exist on LEFT side
        validateTableEnvironment("assorted_test_db", "acid_01", Environment.LEFT);
        validateTableEnvironment("assorted_test_db", "acid_02", Environment.LEFT);
        validateTableEnvironment("assorted_test_db", "acid_03", Environment.LEFT);
        validateTableEnvironment("assorted_test_db", "ext_part_01", Environment.LEFT);
        validateTableEnvironment("assorted_test_db", "ext_part_02", Environment.LEFT);
        validateTableEnvironment("assorted_test_db", "legacy_mngd_01", Environment.LEFT);

        validateTableEnvironment("assorted_test_db", "ext_missing_01", Environment.RIGHT);

    }

    @Test
    public void checkTotalPhaseCount() {
        // Validate total phase count for tables
        validateTablePhaseTotalCount("assorted_test_db", "acid_01", 3);
        validateTablePhaseTotalCount("assorted_test_db", "ext_part_01", 3);
    }

    @Test
    public void checkPhaseSummary() {
        // Validate phase summary shows all tables in CALCULATED_SQL phase
        validateDBInPhaseSummaryCount( "assorted_test_db", PhaseState.PROCESSED, 7);
    }

}
