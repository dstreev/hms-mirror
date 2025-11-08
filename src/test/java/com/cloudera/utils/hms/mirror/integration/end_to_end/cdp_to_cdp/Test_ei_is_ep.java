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

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = Mirror.class,
        args = {
                "--hms-mirror.config.data-strategy=EXPORT_IMPORT",
                "--hms-mirror.config.export-partition-count=500",
                "--hms-mirror.conversion.test-filename=/test_data/assorted_tbls_01.yaml",
                "--hms-mirror.config.intermediate-storage=s3a://my_is_bucket",
                "--hms-mirror.config.filename=/config/default.yaml.cdp-cdp",
                "--hms-mirror.config.output-dir=${user.home}/.hms-mirror/test-output/e2e/cdp_cdp/ei_is_ep"
        })
@Slf4j
public class Test_ei_is_ep extends E2EBaseTest {

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
        // Validate database location on RIGHT
        validateDBLocation("assorted_test_db", Environment.RIGHT, 
                "hdfs://HOME90/warehouse/tablespace/external/hive/assorted_test_db.db");
    }

    @Test
    public void ext_part_01_phaseTest() {
        // Validate phase state for ext_part_01
        validatePhase("assorted_test_db", "ext_part_01", PhaseState.PROCESSED);
        validatePhase("assorted_test_db", "ext_part_02", PhaseState.PROCESSED);
        validatePhase("assorted_test_db", "legacy_mngd_01", PhaseState.PROCESSED);
        validatePhase("assorted_test_db", "ext_missing_01", PhaseState.PROCESSED);
    }

    @Test
    public void ext_part_01_partitionCountTest() {
        // Validate partition count for ext_part_01 on LEFT
        // Based on the yaml, ext_part_01 has 440 partitions
        validatePartitionCount("assorted_test_db", "ext_part_01", Environment.LEFT, 440);
    }

    @Test
    public void checkExportImportStrategy() {
        // Validate that all tables use EXPORT_IMPORT strategy
        validateTableStrategy("assorted_test_db", "ext_part_01", DataStrategyEnum.EXPORT_IMPORT);
        validateTableStrategy("assorted_test_db", "ext_part_02", DataStrategyEnum.EXPORT_IMPORT);
        validateTableStrategy("assorted_test_db", "legacy_mngd_01", DataStrategyEnum.EXPORT_IMPORT);
        validateTableStrategy("assorted_test_db", "ext_missing_01", DataStrategyEnum.EXPORT_IMPORT);

    }

    @Test
    public void checkIntermediateStorageUsed() {
        // Validate that intermediate storage is used in EXPORT commands
        // The actual timestamp in the path will vary, so we check the pattern
//        boolean found = false;
        validateTableSqlAction("assorted_test_db", "ext_part_01", Environment.LEFT, 
                "EXPORT TABLE ext_part_01 TO \"s3a://my_is_bucket/hms_mirror_working/");
        validateTableSqlAction("assorted_test_db", "ext_part_02", Environment.LEFT,
                "/assorted_test_db/ext_part_02");

    }

    @Test
    public void checkImportTableSql() {
        // Validate IMPORT command exists on RIGHT side for ext_part_01
        // Note: For EXTERNAL tables, the IMPORT command includes EXTERNAL keyword and LOCATION
//        boolean found = false;
        validateTableSqlAction("assorted_test_db", "ext_part_01", Environment.RIGHT,
                "IMPORT EXTERNAL TABLE ext_part_01 FROM \"s3a://my_is_bucket/hms_mirror_working/");
        validateTableSqlAction("assorted_test_db", "ext_part_01", Environment.RIGHT,
                "/assorted_test_db/ext_part_01");
        validateTableSqlAction("assorted_test_db", "ext_part_01", Environment.RIGHT,
                "LOCATION \"hdfs://HOME90/warehouse/tablespace/external/hive/assorted_test_db.db/ext_part_01\"");

    }

    @Test
    public void checkTableDefinitions() {
        // Validate that table definitions exist on LEFT
        validateTableEnvironmentHasDefinition("assorted_test_db", "ext_part_01", Environment.LEFT);
        validateTableEnvironmentHasDefinition("assorted_test_db", "ext_part_02", Environment.LEFT);
        validateTableEnvironmentHasDefinition("assorted_test_db", "legacy_mngd_01", Environment.LEFT);
    }

    @Test
    public void checkTableProperties() {
        // Validate specific table properties for ext_part_01
        validateTableProperty("assorted_test_db", "ext_part_01", Environment.LEFT, 
                "bucketing_version", "2");
    }

    @Test
    public void checkPartitionLocations() {
        // Validate some specific partition locations for ext_part_01
        validatePartitionLocation("assorted_test_db", "ext_part_01", Environment.LEFT,
                "num=50", "hdfs://HDP50/warehouse/tablespace/external/hive/assorted_test_db.db/ext_part_01/num=50");
        validatePartitionLocation("assorted_test_db", "ext_part_01", Environment.LEFT,
                "num=100", "hdfs://HDP50/warehouse/tablespace/external/hive/assorted_test_db.db/ext_part_01/num=100");
        validatePartitionLocation("assorted_test_db", "ext_part_01", Environment.LEFT,
                "num=200", "hdfs://HDP50/warehouse/tablespace/external/hive/assorted_test_db.db/ext_part_01/num=200");
    }

    @Test
    public void checkDatabaseSql() {
        // Validate database creation SQL on RIGHT
        validateDBSqlPair("assorted_test_db", Environment.RIGHT, "Create Database",
                "CREATE DATABASE IF NOT EXISTS assorted_test_db");
        validateDBSqlPair("assorted_test_db", Environment.RIGHT, "Alter Database Location",
                "ALTER DATABASE assorted_test_db SET LOCATION \"hdfs://HOME90/warehouse/tablespace/external/hive/assorted_test_db.db\"");

    }

    @Test
    public void checkTableCreateStrategy() {
        // Validate create strategy for tables
        // LEFT side should not be modified (NOTHING)
        validateTableEnvironmentCreateStrategy("assorted_test_db", "ext_part_01", Environment.LEFT,
                CreateStrategy.NOTHING);
        validateTableEnvironmentCreateStrategy("assorted_test_db", "ext_part_01", Environment.RIGHT,
                CreateStrategy.NOTHING);
    }

    @Test
    public void validateTableExistence() {
        // Validate that tables exist on LEFT side
        TableMirror tableMirror = getTableMirrorOrFail("assorted_test_db", "ext_part_01");
        TableMirror tableMirror2 = getTableMirrorOrFail("assorted_test_db", "ext_part_02");
        TableMirror tableMirror3 = getTableMirrorOrFail("assorted_test_db", "legacy_mngd_01");
    }

}
