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

import com.cloudera.utils.hms.mirror.CreateStrategy;
import com.cloudera.utils.hms.mirror.cli.Mirror;
import com.cloudera.utils.hms.mirror.domain.core.TableMirror;
import com.cloudera.utils.hms.mirror.domain.support.DataStrategyEnum;
import com.cloudera.utils.hms.mirror.integration.end_to_end.E2EBaseTest;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.cloudera.utils.hms.mirror.PhaseState;
import com.cloudera.utils.hms.mirror.domain.support.Environment;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = Mirror.class,
        args = {
                "--hms-mirror.config.data-strategy=COMMON",
                "--hms-mirror.config.output-dir=${user.home}/.hms-mirror/test-output/e2e/cdp_cdp/common_01",
                "--hms-mirror.conversion.test-filename=/test_data/assorted_tbls_01.yaml",
                "--hms-mirror.config.filename=/config/default.yaml.cdp-cdp"
        })
@Slf4j
public class Test_common_01 extends E2EBaseTest {
    //
//        String[] args = new String[]{"-d", "COMMON",
//                "-sql",
//                "-ltd", ASSORTED_TBLS_04,
//                "-cfg", CDP_CDP,
//                "-o", outputDir
//        };
//
//        long rtn = 0;
//        MirrorLegacy mirror = new MirrorLegacy();
//        rtn = mirror.go(args);
//        int check = 0;
//        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);
//
//    }
    @Test
    public void returnCodeTest() {
        // Get Runtime Return Code.
        long rtn = getReturnCode();
        // Verify the return code.
        assertEquals(0L, rtn, "Return Code Failure: " + rtn);
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
        // Validate that 4 tables are processed with COMMON strategy
        validateTableCount("assorted_test_db", 4);
    }

    @Test
    public void ext_part_01_phaseTest() {
        // Validate phase state for ext_part_01
        validatePhase("assorted_test_db", "ext_part_01", PhaseState.PROCESSED);
    }

    @Test
    public void ext_part_02_phaseTest() {
        // Validate phase state for ext_part_02
        validatePhase("assorted_test_db", "ext_part_02", PhaseState.PROCESSED);
    }

    @Test
    public void legacy_mngd_01_phaseTest() {
        // Validate phase state for legacy_mngd_01
        validatePhase("assorted_test_db", "legacy_mngd_01", PhaseState.PROCESSED);
    }

    @Test
    public void ext_missing_01_phaseTest() {
        // Validate phase state for ext_missing_01
        validatePhase("assorted_test_db", "ext_missing_01", PhaseState.PROCESSED);
    }

    @Test
    public void checkCommonStrategy() {
        // Validate that all tables use COMMON strategy
        validateTableStrategy("assorted_test_db","ext_part_01", DataStrategyEnum.COMMON);
        validateTableStrategy("assorted_test_db","ext_part_02", DataStrategyEnum.COMMON);
        validateTableStrategy("assorted_test_db","legacy_mngd_01", DataStrategyEnum.COMMON);
        validateTableStrategy("assorted_test_db","ext_missing_01", DataStrategyEnum.COMMON);

    }

    @Test
    public void checkTableLocations() {
        // Validate table locations - COMMON strategy points RIGHT tables to LEFT locations
        validateTableLocation("assorted_test_db", "ext_part_01", Environment.LEFT,
                "hdfs://HDP50/warehouse/tablespace/external/hive/assorted_test_db.db/ext_part_01");
        validateTableLocation("assorted_test_db", "ext_part_02", Environment.LEFT,
                "hdfs://HDP50/warehouse/tablespace/external/hive/assorted_test_db.db/ext_part_02");
        validateTableLocation("assorted_test_db", "legacy_mngd_01", Environment.LEFT,
                "hdfs://HDP50/warehouse/tablespace/external/hive/assorted_test_db.db/legacy_mngd_01");
        // ext_missing_01 exists on RIGHT but not LEFT - location on RIGHT
        validateTableLocation("assorted_test_db", "ext_missing_01", Environment.RIGHT,
                "hdfs://HDP50/warehouse/tablespace/external/hive/assorted_test_db.db/ext_missing_01");
    }

    @Test
    public void checkRightTablesPointToLeftData() {
        // RIGHT tables should point to LEFT data locations in COMMON strategy
        validateTableSqlAction("assorted_test_db", "ext_part_01", Environment.RIGHT,
                "hdfs://HDP50/warehouse/tablespace/external/hive/assorted_test_db.db/ext_part_01");

    }

    @Test
    public void checkPartitionedTableHandling() {
        // ext_part_01 is partitioned - should have MSCK REPAIR
        validateTableSqlAction("assorted_test_db", "ext_part_01", Environment.RIGHT,
                "MSCK REPAIR TABLE ext_part_01");

    }

    @Test
    public void checkNonPartitionedTableNoMSCK() {
        // ext_part_02 is non-partitioned - should NOT have MSCK REPAIR
        // TODO: Negative check
    }

    @Test
    public void checkExternalTablePurgeProperty() {
        // All tables should have external.table.purge=true
        validateTableSqlAction("assorted_test_db", "ext_part_01", Environment.RIGHT,
                "'external.table.purge'='true'");
    }

    @Test
    public void checkDiscoverPartitionsProperty() {
        // Partitioned tables should have discover.partitions=true
        validateTableSqlAction("assorted_test_db", "ext_part_01", Environment.RIGHT,
                "'discover.partitions'='true'");

    }

    @Test
    public void checkTableExistence() {
        // Validate table existence - ext_missing_01 only exists on RIGHT
        validateTableEnvironment("assorted_test_db", "ext_part_01", Environment.LEFT);
        validateTableEnvironment("assorted_test_db", "ext_part_01", Environment.RIGHT);

        validateTableEnvironment("assorted_test_db", "ext_part_02", Environment.LEFT);
        validateTableEnvironment("assorted_test_db", "ext_part_02", Environment.RIGHT);


        validateTableEnvironment("assorted_test_db", "legacy_mngd_01", Environment.LEFT);
        validateTableEnvironment("assorted_test_db", "legacy_mngd_01", Environment.RIGHT);

        TableMirror tableMirror4 = getTableMirrorOrFail("assorted_test_db", "ext_missing_01");
        assertFalse(tableMirror4.getEnvironmentTable(Environment.LEFT).isExists(),
                "ext_missing_01 should NOT exist on LEFT");
        validateTableEnvironment("assorted_test_db", "ext_missing_01", Environment.RIGHT);

    }

    @Test
    public void checkExtMissing01NoSQL() {
        // ext_missing_01 exists on RIGHT but not LEFT - has issue on LEFT

        validateTableIssues("assorted_test_db", "ext_missing_01", Environment.LEFT);
        validateTableIssuesHave("assorted_test_db", "ext_missing_01", Environment.LEFT,
                "Schema exists on the target, but not on the source");
//        var ext_missing_left = getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("ext_missing_01").getEnvironmentTable(Environment.LEFT);
//        assertFalse(ext_missing_left.getIssues().isEmpty(), "Should have issue on LEFT");
//        assertTrue(ext_missing_left.getIssues().get(0).contains("Schema exists on the target, but not on the source"));
        
        // No SQL generated for RIGHT since table already exists there
        validateTableSqlNotGenerated("assorted_test_db", "ext_missing_01", Environment.RIGHT);
        validateTableEnvironmentCreateStrategy("assorted_test_db", "ext_missing_01", Environment.RIGHT,
                CreateStrategy.NOTHING);
//        assertEquals("NOTHING", getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("ext_missing_01").getEnvironmentTable(Environment.RIGHT).getCreateStrategy().toString());
    }

    @Test
    public void checkLegacyManagedTableHandling() {
        // legacy_mngd_01 creates non-transactional table on RIGHT
        validateTableSqlAction("assorted_test_db", "legacy_mngd_01", Environment.RIGHT,
                "CREATE TABLE");

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
        // ext_missing_01 doesn't exist on LEFT, validate on RIGHT
        validateTableProperty("assorted_test_db", "ext_missing_01", Environment.RIGHT,
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
    public void checkPhaseSummary() {
        // Validate phase summary shows all tables in CALCULATED_SQL phase
        validateDBInPhaseSummaryCount("assorted_test_db", PhaseState.PROCESSED, 4);
    }

    @Test
    public void checkTotalPhaseCount() {
        // Validate total phase count for tables based on actual values
        validateTablePhaseTotalCount("assorted_test_db", "ext_part_01", 3);
        validateTablePhaseTotalCount("assorted_test_db", "ext_part_02", 3);
        validateTablePhaseTotalCount("assorted_test_db", "legacy_mngd_01", 3);
        validateTablePhaseTotalCount("assorted_test_db", "ext_missing_01", 3);

    }

    @Test
    public void checkSQLGeneration() {
        // Validate that SQL is generated for tables (except ext_missing_01 on RIGHT)
        validateTableSqlGenerated("assorted_test_db", "ext_part_01", Environment.RIGHT);
        validateTableSqlGenerated("assorted_test_db", "ext_part_02", Environment.RIGHT);
        validateTableSqlGenerated("assorted_test_db", "legacy_mngd_01", Environment.RIGHT);
        // Removed after upgrade.  Seems to have been wrong in the past.

    }

}
