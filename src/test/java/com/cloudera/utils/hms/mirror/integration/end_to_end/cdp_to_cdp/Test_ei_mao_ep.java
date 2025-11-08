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
                "--hms-mirror.config.migrate-acid-only=true",
                "--hms-mirror.config.export-partition-count=500",
                "--hms-mirror.conversion.test-filename=/test_data/assorted_tbls_01.yaml",
                "--hms-mirror.config.filename=/config/default.yaml.cdp-cdp",
                "--hms-mirror.config.output-dir=${user.home}/.hms-mirror/test-output/e2e/cdp_cdp/ei_mao_ep"
        })
@Slf4j
public class Test_ei_mao_ep extends E2EBaseTest {

    @Test
    public void returnCodeTest() {
        // Get Runtime Return Code.
        long rtn = getReturnCode();
        // Verify the return code is 0 (success)
        long check = 0L;
        assertEquals(check, rtn, "Return Code Failure: " + rtn);
    }

    @Test
    public void databaseLocationTest() {
        // Validate database locations
        validateDBLocation("assorted_test_db", Environment.LEFT,
                "hdfs://HDP50/warehouse/tablespace/external/hive/assorted_test_db.db");
        validateDBLocation("assorted_test_db", Environment.RIGHT,
                "hdfs://HOME90/warehouse/tablespace/external/hive/assorted_test_db.db");
    }

    @Test
    public void tableCountTest() {
        // Validate that only 3 ACID tables are processed with migrate-acid-only
        validateTableCount("assorted_test_db", 3);
    }

    @Test
    public void checkPhaseTest() {
        // Validate phase state for acid_01
        validatePhase("assorted_test_db", "acid_01", PhaseState.PROCESSED);
        validatePhase("assorted_test_db", "acid_03", PhaseState.PROCESSED);
        validatePhase("assorted_test_db", "acid_02", PhaseState.PROCESSED);
    }

    @Test
    public void validateExportImportStrategy() {
        // Validate that all tables use EXPORT_IMPORT strategy
        validateTableStrategy("assorted_test_db", "acid_01", DataStrategyEnum.EXPORT_IMPORT);
        validateTableStrategy("assorted_test_db", "acid_02", DataStrategyEnum.EXPORT_IMPORT);
        validateTableStrategy("assorted_test_db", "acid_03", DataStrategyEnum.EXPORT_IMPORT);
    }

    @Test
    public void acidTableValidationTest() {
        // Validate all tables are ACID tables
        validateTableIsACID("assorted_test_db", "acid_01", Environment.LEFT);
        validateTableIsACID("assorted_test_db", "acid_02", Environment.LEFT);
        validateTableIsACID("assorted_test_db", "acid_03", Environment.LEFT);
    }

    @Test
    public void validateTransactionalProperties() {
        // Validate transactional properties
        validateTableProperty("assorted_test_db", "acid_01", Environment.LEFT,
                "transactional", "true");
        validateTableProperty("assorted_test_db", "acid_01", Environment.LEFT,
                "transactional_properties", "default");
        
        validateTableProperty("assorted_test_db", "acid_02", Environment.LEFT,
                "transactional", "true");
        validateTableProperty("assorted_test_db", "acid_02", Environment.LEFT,
                "transactional_properties", "default");
        
        validateTableProperty("assorted_test_db", "acid_03", Environment.LEFT,
                "transactional", "true");
        validateTableProperty("assorted_test_db", "acid_03", Environment.LEFT,
                "transactional_properties", "default");
    }

    @Test
    public void validateExportPaths() {
        // Validate export paths for ACID tables
        validateTableSqlPair("assorted_test_db", Environment.LEFT, "acid_01", "EXPORT Table",
                "EXPORT TABLE acid_01 TO \"hdfs://HDP50/apps/hive/warehouse/export_assorted_test_db/acid_01\"");
    }

    @Test
    public void validateImportPaths() {
        // Validate import paths for tables
        validateTableSqlPair("assorted_test_db", Environment.RIGHT, "acid_01", "IMPORT Table",
                "IMPORT TABLE acid_01 FROM \"hdfs://HDP50/apps/hive/warehouse/export_assorted_test_db/acid_01\"");
    }

    @Test
    public void validatePartitionCount() {
        // acid_03 has 200 partitions
        // The export-partition-count is set to 500, so this should succeed
        validatePartitionCount("assorted_test_db", "acid_03", Environment.LEFT, 200);
    }

    @Test
    public void validateExportPartitionLimit() {
        // Verify that acid_03 with 200 partitions is within the 500 limit
        // No errors should be present
        validateTableErrorCount("assorted_test_db", "acid_03", Environment.LEFT, 0);
        validateTableIssueCount("assorted_test_db", "acid_03", Environment.LEFT, 0);
    }

    @Test
    public void validateTableLocations() {
        // Validate table locations
        validateTableLocation("assorted_test_db", "acid_01", Environment.LEFT,
                "hdfs://HDP50/warehouse/tablespace/managed/hive/assorted_test_db.db/acid_01");
        validateTableLocation("assorted_test_db", "acid_02", Environment.LEFT,
                "hdfs://HDP50/warehouse/tablespace/managed/hive/assorted_test_db.db/acid_02");
        validateTableLocation("assorted_test_db", "acid_03", Environment.LEFT,
                "hdfs://HDP50/warehouse/tablespace/managed/hive/assorted_test_db.db/acid_03");
    }

    @Test
    public void validateBucketingForACIDTables() {
        // Validate acid_01 has 2 buckets
        validateTableBuckets("assorted_test_db", "acid_01", Environment.LEFT, 2);
        // acid_02 doesn't have buckets
        
        // Validate acid_03 has 6 buckets and is partitioned
        validateTableBuckets("assorted_test_db", "acid_03", Environment.LEFT, 6);
        validatePartitioned("assorted_test_db", "acid_03", Environment.LEFT);
    }

    @Test
    public void validateTableProperties() {
        // Validate bucketing_version for all ACID tables
        validateTableProperty("assorted_test_db", "acid_01", Environment.LEFT,
                "bucketing_version", "2");
        validateTableProperty("assorted_test_db", "acid_02", Environment.LEFT,
                "bucketing_version", "2");
        validateTableProperty("assorted_test_db", "acid_03", Environment.LEFT,
                "bucketing_version", "2");
    }

    @Test
    public void validateDatabaseSql() {
        // Validate database creation SQL on RIGHT
        validateDBSqlPair("assorted_test_db", Environment.RIGHT,"Create Database",
                "CREATE DATABASE IF NOT EXISTS assorted_test_db\n");
        validateDBSqlPair("assorted_test_db", Environment.RIGHT,"Alter Database Location",
                "ALTER DATABASE assorted_test_db SET LOCATION \"hdfs://HOME90/warehouse/tablespace/external/hive/assorted_test_db.db\"");
    }

    @Test
    public void validateTableExistence() {
        // Validate that all ACID tables exist on LEFT
        validateTableEnvironment("assorted_test_db", "acid_01", Environment.LEFT);
        validateTableEnvironment("assorted_test_db", "acid_02", Environment.LEFT);
        validateTableEnvironment("assorted_test_db", "acid_03", Environment.LEFT);

        // Tables don't exist on RIGHT yet (just SQL generated)
        validateTableEnvironmentNotExist("assorted_test_db", "acid_01", Environment.RIGHT);
        validateTableEnvironmentNotExist("assorted_test_db", "acid_02", Environment.RIGHT);
        validateTableEnvironmentNotExist("assorted_test_db", "acid_03", Environment.RIGHT);
    }

    @Test
    public void validateOnlyACIDTablesProcessed() {
        // Verify that only ACID tables are in the output (migrate-acid-only)
        validateTableInDatabase("assorted_test_db", "acid_01");
        validateTableInDatabase("assorted_test_db", "acid_02");
        validateTableInDatabase("assorted_test_db", "acid_03");

        // Non-ACID tables should not be present
        validateTableNotInDatabase("assorted_test_db", "ext_part_01");
        validateTableNotInDatabase("assorted_test_db", "ext_part_02");
        validateTableNotInDatabase("assorted_test_db", "legacy_mngd_01");
        validateTableNotInDatabase("assorted_test_db", "ext_missing_01");
    }

    @Test
    public void checkPhaseSummaryCount() {
        // Validate phase summary shows all tables in CALCULATED_SQL phase
        validateDBInPhaseSummaryCount( "assorted_test_db", PhaseState.PROCESSED, 3);
    }

    @Test
    public void validateTotalPhaseCount() {
        // Validate total phase count for tables
        validateTablePhaseTotalCount("assorted_test_db", "acid_01", 3);
        validateTablePhaseTotalCount("assorted_test_db", "acid_02", 3);
        validateTablePhaseTotalCount("assorted_test_db", "acid_03", 3);
    }

    @Test
    public void validateCurrentPhase() {
        // All tables should be at phase 1
        validateTablePhaseCurrentCount("assorted_test_db", "acid_01", 2);
        validateTablePhaseCurrentCount("assorted_test_db", "acid_02", 2);
        validateTablePhaseCurrentCount("assorted_test_db", "acid_03", 2);
    }

    @Test
    public void validateSQLGeneration() {
        // Validate that SQL is generated for both LEFT and RIGHT
        validateTableSqlGenerated("assorted_test_db", "acid_01", Environment.LEFT);
        validateTableSqlGenerated("assorted_test_db", "acid_01", Environment.RIGHT);

        validateTableSqlGenerated("assorted_test_db", "acid_03", Environment.LEFT);
        validateTableSqlGenerated("assorted_test_db", "acid_03", Environment.RIGHT);
    }

    @Test
    public void checkExportPartitionCountConfig() {
        // The test sets export-partition-count to 500
        // This allows acid_03 with 200 partitions to be processed
        assertEquals(500, getConversion().getJob().getHybrid().getExportImportPartitionLimit(),
                "Export partition limit should be set to 500");
    }

}
