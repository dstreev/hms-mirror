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
                "--hms-mirror.config.intermediate-storage=s3a://my_is_bucket",
                "--hms-mirror.conversion.test-filename=/test_data/assorted_tbls_01.yaml",
                "--hms-mirror.config.filename=/config/default.yaml.cdp-cdp",
                "--hms-mirror.config.output-dir=${user.home}/.hms-mirror/test-output/e2e/cdp_cdp/hybrid_ma_da_is"
        })
@Slf4j
public class Test_hybrid_ma_da_is extends E2EBaseTest {
    //        String[] args = new String[]{"-d", "HYBRID",
//                "-ma", "-da", "-is", INTERMEDIATE_STORAGE,
//                "-ltd", ASSORTED_TBLS_04,
//                "-cfg", CDP_CDP,
//                "-o", outputDir
//        };
//
//        long rtn = 0;
//        MirrorLegacy mirror = new MirrorLegacy();
//        rtn = mirror.go(args);
//        int check = 0;
//        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check * -1, check * -1, rtn);
    @Test
    public void returnCodeTest() {
        // Get Runtime Return Code.
        long rtn = getReturnCode();
        // Verify the return code.
        long check = 0L;
        assertEquals(check * -1, rtn, "Return Code Failure: " + rtn);
    }

    @Test
    public void checkTableCount() {
        validateTableCount("assorted_test_db", 7);
//        assertEquals(7, getConversion().getDatabase("assorted_test_db").getTableMirrors().size());
    }

    @Test
    public void checkACIDStrategy() {
        // ACID tables use EXPORT_IMPORT strategy for intermediate storage
        validateTableStrategy("assorted_test_db", "acid_01", DataStrategyEnum.EXPORT_IMPORT);
        validateTableStrategy("assorted_test_db", "acid_02", DataStrategyEnum.EXPORT_IMPORT);
        validateTableStrategy("assorted_test_db", "acid_03", DataStrategyEnum.SQL_INTERMEDIATE);

//        assertEquals("EXPORT_IMPORT", getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("acid_01").getStrategy().toString());
//        assertEquals("EXPORT_IMPORT", getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("acid_02").getStrategy().toString());
        // acid_03 is partitioned so uses SQL strategy
//        assertEquals("SQL", getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("acid_03").getStrategy().toString());
    }

    @Test
    public void checkNonACIDStrategy() {
        // Non-ACID tables use different strategies based on partitioning  
        // ext_part_01 is partitioned so uses SQL strategy
        validateTableStrategy("assorted_test_db", "ext_part_01", DataStrategyEnum.SQL_INTERMEDIATE);
        validateTableStrategy("assorted_test_db", "ext_part_02", DataStrategyEnum.EXPORT_IMPORT);
        validateTableStrategy("assorted_test_db", "legacy_mngd_01", DataStrategyEnum.EXPORT_IMPORT);

//        assertEquals("SQL", getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("ext_part_01").getStrategy().toString());
        // Non-partitioned tables use EXPORT_IMPORT
    }

    @Test
    public void checkIntermediateStorage() {
        // Validate intermediate storage is used in EXPORT statements
        validateTableSqlAction("assorted_test_db", "acid_01", Environment.LEFT,
                "EXPORT TABLE acid_01 TO");
        validateTableSqlAction("assorted_test_db", "acid_01", Environment.LEFT,
                "s3a://my_is_bucket/hms_mirror_working");

    }

    @Test
    public void checkImportWithLocation() {
        // IMPORT statements should include LOCATION clause for external tables
//        validateTableSqlPair("assorted_test_db", Environment.RIGHT, "ext_part_01",
        validateTableSqlAction("assorted_test_db", "ext_part_02", Environment.RIGHT,
                "IMPORT EXTERNAL TABLE ext_part_02 FROM");
        validateTableSqlAction("assorted_test_db", "ext_part_02", Environment.RIGHT,
                "LOCATION");
        validateTableSqlAction("assorted_test_db", "ext_part_02", Environment.RIGHT,
                "hdfs://HOME90");

    }

    @Test
    public void checkShadowTables() {
        // SQL strategy tables should have shadow tables pointing to intermediate storage
        validateTableSqlAction("assorted_test_db", "acid_03", Environment.RIGHT,
                "s3a://my_is_bucket/hms_mirror_working");

    }

    @Test
    public void checkTransferTables() {
        // SQL strategy should create TRANSFER tables in intermediate storage
        validateTableSqlAction("assorted_test_db", "acid_03", LEFT,
                "s3a://my_is_bucket/hms_mirror_working");

    }

    @Test
    public void checkOriginalLocationPreservation() {
        // With intermediate storage, final RIGHT tables may have empty definitions
        // Data is accessed via the original LEFT cluster location
        validateTableEnvironment("assorted_test_db", "acid_03", RIGHT);
        validateTableStrategy("assorted_test_db", "acid_03", DataStrategyEnum.SQL_INTERMEDIATE);

//        var acid03Right = getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("acid_03").getEnvironmentTable(Environment.RIGHT);

        // For intermediate storage, RIGHT environment exists but definition may be empty
//        assertNotNull(acid03Right, "RIGHT environment should exist");

        // The strategy should be SQL for partitioned ACID tables
//        assertEquals("SQL", getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("acid_03").getStrategy().toString());
    }

    @Test
    public void checkDowngradedACIDProperties() {
        // Downgraded ACID tables should have appropriate properties
        validateTableEnvironmentAddPropertiesHas("assorted_test_db", "acid_03", RIGHT,
                "downgraded_from_acid");
        validateTableEnvironmentAddPropertiesHas("assorted_test_db", "acid_03", RIGHT,
                "external.table.purge");

    }

    @Test
    public void checkLinkTestSkipping() {
        // Configuration should skip link tests due to intermediate storage
        // This is evidenced by the warning in the output
        // We can validate this by checking that the process completed successfully
        assertEquals(0L, getReturnCode(), "Process should complete successfully despite skipped link test");
    }

    @Test
    public void checkPhaseStates() {
        // Validate phase states for different table types
        validatePhase("assorted_test_db", "acid_01", PhaseState.PROCESSED);
        validatePhase("assorted_test_db", "acid_02", PhaseState.PROCESSED);
        validatePhase("assorted_test_db", "acid_03", PhaseState.PROCESSED);
        validatePhase("assorted_test_db", "ext_part_01", PhaseState.PROCESSED);
        validatePhase("assorted_test_db", "ext_part_02", PhaseState.PROCESSED);
    }

    @Test
    public void checkShadowTableProperties() {
        // Shadow tables should have hms-mirror_shadow_table property

        validateTableEnvironmentDefinitionHas("assorted_test_db", "acid_03", SHADOW,
                "'hms-mirror_shadow_table'='true'");

//        var acid03Shadow = getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("acid_03").getEnvironmentTable(SHADOW);

        // Check in addProperties
//        boolean foundInProperties = acid03Shadow.getAddProperties().containsKey("hms-mirror_shadow_table");

        // Check in definition

    }

    @Test
    public void checkDataMovementSQL() {
        // Validate data movement from TRANSFER to SHADOW tables
        validateTableSqlAction("assorted_test_db", "acid_03", LEFT,
                "INSERT OVERWRITE TABLE hms_mirror_transfer_acid_03");

    }

    @Test
    public void checkMissingTableHandling() {
        // ext_missing_01 should be handled appropriately
        validateTableSqlNotGenerated("assorted_test_db", "ext_missing_01", RIGHT);
        validateTableIssues("assorted_test_db","ext_missing_01", RIGHT);

    }

    @Test
    public void checkAllTablesProcessed() {
        // Ensure all expected tables are present in the conversion
        TableMirror tableMirror = getTableMirrorOrFail("assorted_test_db", "acid_01");
        TableMirror tableMirror2 = getTableMirrorOrFail("assorted_test_db", "acid_02");
        TableMirror tableMirror3 = getTableMirrorOrFail("assorted_test_db", "acid_03");
        TableMirror tableMirror4 = getTableMirrorOrFail("assorted_test_db", "ext_part_01");
        TableMirror tableMirror5 = getTableMirrorOrFail("assorted_test_db", "ext_part_02");
        TableMirror tableMirror6 = getTableMirrorOrFail("assorted_test_db", "legacy_mngd_01");
        TableMirror tableMirror7 = getTableMirrorOrFail("assorted_test_db", "ext_missing_01");

    }

}
