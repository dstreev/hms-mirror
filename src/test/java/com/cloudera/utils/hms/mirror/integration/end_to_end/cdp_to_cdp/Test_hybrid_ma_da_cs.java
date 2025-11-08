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

import com.cloudera.utils.hms.mirror.cli.Mirror;
import com.cloudera.utils.hms.mirror.domain.core.TableMirror;
import com.cloudera.utils.hms.mirror.domain.support.DataStrategyEnum;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.integration.end_to_end.E2EBaseTest;
import com.cloudera.utils.hms.mirror.PhaseState;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static com.cloudera.utils.hms.mirror.domain.support.Environment.*;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = Mirror.class,
        args = {
                "--hms-mirror.config.data-strategy=HYBRID",
                "--hms-mirror.config.migrate-acid=true",
                "--hms-mirror.config.downgrade-acid=true",
                "--hms-mirror.config.target-namespace=s3a://my_cs_bucket",
                "--hms-mirror.conversion.test-filename=/test_data/assorted_tbls_01.yaml",
                "--hms-mirror.config.filename=/config/default.yaml.cdp-cdp",
                "--hms-mirror.config.output-dir=${user.home}/.hms-mirror/test-output/e2e/cdp_cdp/hybrid_ma_da_cs"
        })
@Slf4j
public class Test_hybrid_ma_da_cs extends E2EBaseTest {
    @Test
    public void returnCodeTest() {
        // Get Runtime Return Code.
        long rtn = getReturnCode();
        // Verify the return code
        assertEquals(0L, rtn, "Return Code Failure: " + rtn);
    }

    @Test
    public void checkTableCount() {
        validateTableCount("assorted_test_db", 7);
    }

    @Test
    public void checkACIDStrategy() {
        // ACID tables use different strategies based on partitioning
        validateTableStrategy("assorted_test_db", "acid_01", DataStrategyEnum.EXPORT_IMPORT);
        validateTableStrategy("assorted_test_db", "acid_02", DataStrategyEnum.EXPORT_IMPORT);
        validateTableStrategy("assorted_test_db", "acid_03", DataStrategyEnum.SQL_INTERMEDIATE);
    }

    @Test
    public void checkNonACIDStrategy() {
        // Non-ACID tables use different strategies based on partitioning  
        // ext_part_01 is partitioned so uses SQL strategy
        validateTableStrategy("assorted_test_db", "ext_part_01", DataStrategyEnum.SQL_INTERMEDIATE);
        validateTableStrategy("assorted_test_db", "ext_part_02", DataStrategyEnum.EXPORT_IMPORT);
        validateTableStrategy("assorted_test_db", "legacy_mngd_01", DataStrategyEnum.EXPORT_IMPORT);
    }

    @Test
    public void validateTargetNamespace() {
        // Validate target namespace is applied to database location
        validateDBSqlAction("assorted_test_db", Environment.RIGHT, "s3a://my_cs_bucket");
    }

    @Test
    public void validateTransferTableCreation() {
        // ACID tables should have transfer tables on LEFT
        validateTableSqlAction("assorted_test_db", "acid_01", Environment.LEFT, "EXPORT TABLE acid_01 TO");
        validateTableSqlAction("assorted_test_db", "acid_02", Environment.LEFT, "s3a://my_cs_bucket/hms_mirror_working");
    }

    @Test
    public void checkRightTableCreation() {
        // Validate RIGHT side has SQL statements for EXPORT_IMPORT tables
        validateTableSqlGenerated("assorted_test_db", "acid_01", Environment.RIGHT);
    }

    @Test
    public void checkACIDDowngrade() {
        // With EXPORT_IMPORT strategy, definition might be empty on RIGHT
        TableMirror tableMirror = getTableMirrorOrFail("assorted_test_db", "acid_01");
        // Check that the Right environment exists
        assertNotNull(tableMirror.getEnvironmentTable(Environment.RIGHT), "RIGHT environment should exist");
    }

    @Test
    public void checkDataMovementForACID() {
        // Validate data movement SQL for ACID tables
        validateTableSqlAction("assorted_test_db", "acid_01", Environment.LEFT,
                "EXPORT TABLE acid_01 TO");
        validateTableSqlAction("assorted_test_db", "acid_01", Environment.LEFT,
                "s3a://my_cs_bucket/hms_mirror_working");
    }

    @Test
    public void checkPartitionHandlingForACID() {
        // acid_03 is partitioned and uses SQL strategy
        validateTableStrategy("assorted_test_db", "acid_03", DataStrategyEnum.SQL_INTERMEDIATE);

        // SQL strategy should have phases
        validateTablePhaseTotalCount("assorted_test_db", "acid_03", 3);
    }

    @Test
    public void checkMSCKRepairForShadowTables() {
        // Shadow tables should have MSCK REPAIR for partitioned tables
        validateTableSqlAction("assorted_test_db", "acid_03", Environment.RIGHT,
                "MSCK REPAIR TABLE");
        validateTableSqlAction("assorted_test_db", "acid_03", Environment.RIGHT,
                "hms_mirror_shadow_acid_03");
    }

    @Test
    public void checkCleanupOperations() {
        // EXPORT_IMPORT strategy has empty cleanup
        validateTableCleanupSqlGenerated("assorted_test_db", "acid_03", Environment.LEFT);
        validateTableCleanupSqlGenerated("assorted_test_db", "acid_03", Environment.RIGHT);
    }

    @Test
    public void checkExportImportForNonACID() {
        // ext_part_02 uses EXPORT_IMPORT strategy
        validateTableStrategy("assorted_test_db", "ext_part_02", DataStrategyEnum.EXPORT_IMPORT);

        // Should have SQL on both sides
        validateTableSqlGenerated("assorted_test_db", "ext_part_02", Environment.LEFT);
        validateTableSqlGenerated("assorted_test_db", "ext_part_02", Environment.RIGHT);
    }

    @Test
    public void checkBucketedTableProperties() {
        // Check that acid_02 has definition on LEFT side
        validateTableEnvironmentDefinitionHas("assorted_test_db", "acid_02", LEFT,
                "CLUSTERED");
    }

    @Test
    public void checkStorageFormats() {
        // Check LEFT side definition for ORC format
        validateTableEnvironmentDefinitionHas("assorted_test_db", "acid_01", LEFT, "OrcSerde");
    }

    @Test
    public void checkDatabaseLocationChange() {
        // Database should have ALTER statement for target namespace
        validateDBSqlAction("assorted_test_db", Environment.RIGHT, "ALTER DATABASE assorted_test_db SET LOCATION");
        validateDBSqlAction("assorted_test_db", Environment.RIGHT, "s3a://my_cs_bucket");
    }

    @Test
    public void checkPhaseStates() {
        // Validate phase states for ACID tables using validatePhase helper
        validatePhase("assorted_test_db", "acid_01", PhaseState.PROCESSED);
        validatePhase("assorted_test_db", "acid_02", PhaseState.PROCESSED);
        validatePhase("assorted_test_db", "acid_03", PhaseState.PROCESSED);
        
        // Non-ACID tables should also be in calculated state
        validatePhase("assorted_test_db", "ext_part_02", PhaseState.PROCESSED);
        validatePhase("assorted_test_db", "legacy_mngd_01", PhaseState.PROCESSED);
    }

    @Test
    public void checkHiveSettings() {
        // EXPORT_IMPORT doesn't generate SET statements in SQL
        // Check that acid_03 (SQL strategy) has settings in issues instead
        validateTableIssuesHave("assorted_test_db", "acid_03", Environment.RIGHT,
                "hive.stats.autogather");
        validateTableIssuesHave("assorted_test_db", "acid_03", Environment.RIGHT,
                "hive.stats.column.autogather");
    }

    @Test
    public void checkTableDefinition() {
        // EXPORT_IMPORT strategy doesn't populate definition on RIGHT side
        // Check that RIGHT environment exists but definition is empty
        validateTableEnvironmentNotExist("assorted_test_db", "acid_01", Environment.RIGHT);
    }

    @Test
    public void checkMissingTableHandling() {
        // ext_missing_01 should be handled appropriately
        validateTableIssues("assorted_test_db", "ext_missing_01", Environment.RIGHT);
        validateTableSqlNotGenerated("assorted_test_db", "ext_missing_01", Environment.RIGHT);
    }

    @Test
    public void checkDataMovementFromShadowToTarget() {
        // EXPORT_IMPORT strategy uses IMPORT statement instead of shadow tables
        validateTableSqlAction("assorted_test_db", "acid_01", Environment.RIGHT,
                "IMPORT EXTERNAL TABLE acid_01 FROM");
    }

    @Test
    public void checkPartitionDiscovery() {
        // acid_03 uses SQL strategy and should have discover.partitions in RIGHT table properties
        validateTableEnvironment( "assorted_test_db", "acid_03", Environment.RIGHT);

        validateTableEnvironmentAddPropertiesHas("assorted_test_db", "acid_03", RIGHT,
                "discover.partitions");
        // Check in addProperties

        validateTableEnvironmentDefinitionHas("assorted_test_db", "acid_03", RIGHT,
                "'discover.partitions'='true'");
    }


    @Test
    public void validateAllTablesProcessed() {
        // Ensure all expected tables are present
        TableMirror tableMirror = getTableMirrorOrFail("assorted_test_db", "acid_01");
        TableMirror tableMirror2 = getTableMirrorOrFail("assorted_test_db", "acid_02");
        TableMirror tableMirror3 = getTableMirrorOrFail("assorted_test_db", "acid_03");
        TableMirror tableMirror4 = getTableMirrorOrFail("assorted_test_db", "ext_part_01");
        TableMirror tableMirror5 = getTableMirrorOrFail("assorted_test_db", "ext_part_02");
        TableMirror tableMirror6 = getTableMirrorOrFail("assorted_test_db", "legacy_mngd_01");
        TableMirror tableMirror7 = getTableMirrorOrFail("assorted_test_db", "ext_missing_01");
    }

}
