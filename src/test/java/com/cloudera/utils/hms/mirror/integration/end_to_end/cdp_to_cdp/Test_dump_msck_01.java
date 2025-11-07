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
import com.cloudera.utils.hms.mirror.domain.core.TableMirror;
import com.cloudera.utils.hms.mirror.domain.support.DataStrategyEnum;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.integration.end_to_end.E2EBaseTest;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = Mirror.class,
        args = {
                "--hms-mirror.config.data-strategy=DUMP",
                "--hms-mirror.conversion.test-filename=/test_data/assorted_tbls_01.yaml",
                "--hms-mirror.config.filename=/config/default.yaml.cdp-cdp.msck",
                "--hms-mirror.config.output-dir=${user.home}/.hms-mirror/test-output/e2e/cdp_cdp/dump_01_msck"
        })
@Slf4j
public class Test_dump_msck_01 extends E2EBaseTest {
    @Test
    public void returnCodeTest() {
        // Get Runtime Return Code.
        long rtn = getReturnCode();
        // Verify the return code.
        assertEquals(0L, rtn, "Return Code Failure: " + rtn);
    }

    @Test
    public void databaseLocationTest() {
        // Validate database location on LEFT only (DUMP strategy)
        validateDBLocation("assorted_test_db", Environment.LEFT,
                "hdfs://HDP50/warehouse/tablespace/external/hive/assorted_test_db.db");
    }

    @Test
    public void tableCountTest() {
        // Validate that 4 tables are processed with DUMP strategy
        validateTableCount("assorted_test_db", 4);
    }

    @Test
    public void checkTablePhaseTest() {
        // Validate phase state for ext_part_01
        validatePhase("assorted_test_db", "ext_part_01", PhaseState.PROCESSED);
        validatePhase("assorted_test_db", "ext_part_02", PhaseState.PROCESSED);
        validatePhase("assorted_test_db", "legacy_mngd_01", PhaseState.PROCESSED);
        validatePhase("assorted_test_db", "ext_missing_01", PhaseState.PROCESSED);
    }

    @Test
    public void checkExtMissingHandling() {
        // ext_missing_01 doesn't exist on LEFT - validate empty SQL generation
        TableMirror tableMirror = getTableMirrorOrFail("assorted_test_db", "ext_missing_01");
        assertFalse(tableMirror.getEnvironmentTable(Environment.LEFT).isExists(), "ext_missing_01 should not exist on LEFT");
    }

    @Test
    public void checkDumpStrategy() {
        // Validate that all tables use DUMP strategy
        validateTableStrategy("assorted_test_db", "ext_part_01", DataStrategyEnum.DUMP);
        validateTableStrategy("assorted_test_db", "ext_part_02", DataStrategyEnum.DUMP);
        validateTableStrategy("assorted_test_db", "legacy_mngd_01", DataStrategyEnum.DUMP);
        validateTableStrategy("assorted_test_db", "ext_missing_01", DataStrategyEnum.DUMP);
    }

    @Test
    public void checkTableLocations() {
        // Validate table locations on LEFT for tables that exist
        validateTableLocation("assorted_test_db", "ext_part_01", Environment.LEFT,
                "hdfs://HDP50/warehouse/tablespace/external/hive/assorted_test_db.db/ext_part_01");
        validateTableLocation("assorted_test_db", "ext_part_02", Environment.LEFT,
                "hdfs://HDP50/warehouse/tablespace/external/hive/assorted_test_db.db/ext_part_02");
        validateTableLocation("assorted_test_db", "legacy_mngd_01", Environment.LEFT,
                "hdfs://HDP50/warehouse/tablespace/external/hive/assorted_test_db.db/legacy_mngd_01");
        // ext_missing_01 doesn't exist on LEFT
    }

    @Test
    public void checkDumpGeneratesLeftSQL() {
        // DUMP strategy generates SQL on LEFT side only
        validateTableSqlGenerated("assorted_test_db", "ext_part_01", Environment.LEFT);

        // Verify CREATE TABLE exists
        validateTableSqlAction("assorted_test_db","ext_part_01", Environment.LEFT,
                "CREATE EXTERNAL TABLE `ext_part_01`");
    }

    @Test
    public void checkPartitionAddStatements() {
        // ext_part_01 is partitioned - should have ALTER TABLE ADD PARTITION statements
        validateTableSqlAction("assorted_test_db","ext_part_01", Environment.LEFT,
                "MSCK REPAIR TABLE ext_part_01");
    }

    @Test
    public void checkNoRightSQL() {
        // DUMP strategy should not generate RIGHT side SQL
        validateTableSqlNotGenerated("assorted_test_db", "ext_part_01", Environment.RIGHT);
    }

    @Test
    public void checkTableExistence() {
        // Validate table existence on LEFT
        validateTableEnvironment("assorted_test_db", "ext_part_01", Environment.LEFT);
        validateTableEnvironment("assorted_test_db", "ext_part_02", Environment.LEFT);
        validateTableEnvironment("assorted_test_db", "legacy_mngd_01", Environment.LEFT);
        validateTableEnvironmentNotExist("assorted_test_db", "ext_missing_01", Environment.LEFT);
    }

    @Test
    public void checkPartitionCount() {
        // ext_part_01 has 440 partitions
        validatePartitionCount("assorted_test_db", "ext_part_01", Environment.LEFT, 440);
    }

    @Test
    public void checkTableProperties() {
        // Validate bucketing_version for tables that exist on LEFT
        validateTableProperty("assorted_test_db", "ext_part_01", Environment.LEFT,
                "bucketing_version", "2");
        validateTableProperty("assorted_test_db", "ext_part_02", Environment.LEFT,
                "bucketing_version", "2");
        validateTableProperty("assorted_test_db", "legacy_mngd_01", Environment.LEFT,
                "bucketing_version", "2");
        // ext_missing_01 doesn't exist on LEFT so no properties to validate
    }

    @Test
    public void checkDatabaseSQL() {
        // Validate database creation SQL on LEFT
        validateDBSqlGenerated("assorted_test_db", Environment.LEFT);

        validateDBSqlAction("assorted_test_db", Environment.LEFT, "CREATE DATABASE IF NOT EXISTS assorted_test_db");
        validateDBSqlAction("assorted_test_db", Environment.LEFT, "hdfs://HDP50/warehouse/tablespace/external/hive/assorted_test_db.db");

    }

    @Test
    public void checkPhaseSummary() {
        // Validate phase summary shows all tables in CALCULATED_SQL phase
        validateDBInPhaseSummaryCount("assorted_test_db", PhaseState.PROCESSED, 4);
    }

    @Test
    public void checkTotalPhaseCount() {
        // Validate total phase count for tables
        validateTablePhaseTotalCount("assorted_test_db", "ext_part_01", 3);
        validateTablePhaseTotalCount("assorted_test_db", "ext_part_02", 3);
        validateTablePhaseTotalCount("assorted_test_db", "legacy_mngd_01", 3);
        validateTablePhaseTotalCount("assorted_test_db", "ext_missing_01", 3);
    }

    @Test
    public void checkPartitionedTable() {
        // ext_part_02 is non-partitioned - should NOT have partition statements
        validateTableSqlDescription("assorted_test_db", "ext_part_01", Environment.LEFT,
                "Repairing");
    }

    @Test
    public void checkTableDefinitions() {
        // Validate that table definitions are captured
        validateTableEnvironmentDefinitionHas("assorted_test_db", "ext_part_01", Environment.LEFT,
                "CREATE EXTERNAL TABLE");
        validateTableEnvironmentDefinitionHas("assorted_test_db", "ext_part_01", Environment.LEFT,
                "PARTITIONED BY");
        validateTableEnvironmentDefinitionHas("assorted_test_db", "legacy_mngd_01", Environment.LEFT,
                "CREATE TABLE");
    }

    @Test
    public void checkTableLocation() {
        validateTableLocation("assorted_test_db", "ext_part_02", Environment.LEFT,
                "hdfs://HDP50/warehouse/tablespace/external/hive/assorted_test_db.db/ext_part_02");
    }

}
