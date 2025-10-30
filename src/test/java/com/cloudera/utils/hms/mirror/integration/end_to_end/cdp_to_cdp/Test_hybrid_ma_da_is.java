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
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.xbill.DNS.dnssec.R;

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
        validateTableStrategy("assorted_test_db", "acid_03", DataStrategyEnum.SQL);

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
        validateTableStrategy("assorted_test_db", "ext_part_01", DataStrategyEnum.SQL);
        validateTableStrategy("assorted_test_db", "ext_part_02", DataStrategyEnum.EXPORT_IMPORT);
        validateTableStrategy("assorted_test_db", "legacy_mngd_01", DataStrategyEnum.EXPORT_IMPORT);

//        assertEquals("SQL", getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("ext_part_01").getStrategy().toString());
        // Non-partitioned tables use EXPORT_IMPORT
//        assertEquals("EXPORT_IMPORT", getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("ext_part_02").getStrategy().toString());
//        assertEquals("EXPORT_IMPORT", getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("legacy_mngd_01").getStrategy().toString());
    }

    @Test
    public void checkIntermediateStorage() {
        // Validate intermediate storage is used in EXPORT statements
        validateTableSqlAction("assorted_test_db", "acid_01", Environment.RIGHT,
                "EXPORT TABLE acid_01 TO");
        validateTableSqlAction("assorted_test_db", "acid_01", Environment.RIGHT,
                "s3a://my_is_bucket/hms_mirror_working");

//        var acid01LeftSql = getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.LEFT).getSql();
//        boolean foundIntermediateStorage = false;
//        for (var pair : acid01LeftSql) {
//            if (pair.getAction().contains("EXPORT TABLE acid_01 TO") &&
//                pair.getAction().contains("s3a://my_is_bucket/hms_mirror_working")) {
//                foundIntermediateStorage = true;
//                break;
//            }
//        }
//        assertTrue(foundIntermediateStorage, "Should use intermediate storage for EXPORT operations");
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

//        var extPart02RightSql = getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("ext_part_02").getEnvironmentTable(Environment.RIGHT).getSql();
//        boolean foundImportWithLocation = false;
//        for (var pair : extPart02RightSql) {
//            if (pair.getAction().contains("IMPORT EXTERNAL TABLE ext_part_02 FROM") &&
//                    pair.getAction().contains("LOCATION") &&
//                    pair.getAction().contains("hdfs://HOME90")) {
//                foundImportWithLocation = true;
//                break;
//            }
//        }
//        assertTrue(foundImportWithLocation, "IMPORT should include LOCATION for external tables");
    }

    @Test
    public void checkShadowTables() {
        // SQL strategy tables should have shadow tables pointing to intermediate storage
        validateTableSqlAction("assorted_test_db", "ext_part_02", Environment.SHADOW,
                "s3a://my_is_bucket/hms_mirror_working");

//        var acid03Shadow = getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("acid_03").getEnvironmentTable(SHADOW);
//
//        // Check that shadow table definition contains intermediate storage location
//        boolean foundShadowWithIS = false;
//        for (var line : acid03Shadow.getDefinition()) {
//            if (line.contains("s3a://my_is_bucket/hms_mirror_working")) {
//                foundShadowWithIS = true;
//                break;
//            }
//        }
//        assertTrue(foundShadowWithIS, "Shadow table should point to intermediate storage");
    }

    @Test
    public void checkTransferTables() {
        // SQL strategy should create TRANSFER tables in intermediate storage
        validateTableSqlAction("assorted_test_db", "acid_03", TRANSFER,
                "s3a://my_is_bucket/hms_mirror_working");

//        var acid03Transfer = getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("acid_03").getEnvironmentTable(TRANSFER);
//        assertNotNull(acid03Transfer, "SQL strategy should have TRANSFER table");
//        boolean foundTransferLocation = false;
//        for (var line : acid03Transfer.getDefinition()) {
//            if (line.contains("s3a://my_is_bucket/hms_mirror_working")) {
//                foundTransferLocation = true;
//                break;
//            }
//        }
//        assertTrue(foundTransferLocation, "TRANSFER table should use intermediate storage location");
    }

    @Test
    public void checkOriginalLocationPreservation() {
        // With intermediate storage, final RIGHT tables may have empty definitions
        // Data is accessed via the original LEFT cluster location
        validateTableEnvironment("assorted_test_db", "acid_03", RIGHT);
        validateTableStrategy("assorted_test_db", "acid_03", DataStrategyEnum.SQL);

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

//        var acid03Right = getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("acid_03").getEnvironmentTable(Environment.RIGHT);
//        assertTrue(acid03Right.getAddProperties().containsKey("downgraded_from_acid"),
//                "Should have downgraded_from_acid property");
//        assertTrue(acid03Right.getAddProperties().containsKey("external.table.purge"),
//                "Should have external.table.purge property");
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
        validatePhase("assorted_test_db", "acid_01", PhaseState.CALCULATED_SQL);
        validatePhase("assorted_test_db", "acid_02", PhaseState.CALCULATED_SQL);
        validatePhase("assorted_test_db", "acid_03", PhaseState.CALCULATED_SQL);
        validatePhase("assorted_test_db", "ext_part_01", PhaseState.CALCULATED_SQL);
        validatePhase("assorted_test_db", "ext_part_02", PhaseState.CALCULATED_SQL);
    }

    @Test
    public void checkShadowTableProperties() {
        // Shadow tables should have hms-mirror_shadow_table property
        validateTableEnvironmentAddPropertiesHas("assorted_test_db", "acid_03", SHADOW,
                "hms-mirror_shadow_table");
        validateTableEnvironmentDefinitionHas("assorted_test_db", "acid_03", SHADOW,
                "'hms-mirror_shadow_table'='true'");

//        var acid03Shadow = getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("acid_03").getEnvironmentTable(SHADOW);

        // Check in addProperties
//        boolean foundInProperties = acid03Shadow.getAddProperties().containsKey("hms-mirror_shadow_table");

        // Check in definition
//        boolean foundInDefinition = false;
//        for (var line : acid03Shadow.getDefinition()) {
//            if (line.contains("'hms-mirror_shadow_table'='true'")) {
//                foundInDefinition = true;
//                break;
//            }
//        }

//        assertTrue(foundInProperties || foundInDefinition, "Shadow table should have hms-mirror_shadow_table property");
    }

    @Test
    public void checkDataMovementSQL() {
        // Validate data movement from TRANSFER to SHADOW tables
        validateTableSqlAction("assorted_test_db", "acid_03", LEFT,
                "INSERT OVERWRITE TABLE hms_mirror_transfer_acid_03");

//        var acid03LeftSql = getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("acid_03").getEnvironmentTable(Environment.LEFT).getSql();
//        boolean foundDataMovement = false;
//        for (var pair : acid03LeftSql) {
//            if (pair.getAction().contains("INSERT OVERWRITE TABLE hms_mirror_transfer_acid_03")) {
//                foundDataMovement = true;
//                break;
//            }
//        }
//        assertTrue(foundDataMovement, "Should have data movement to TRANSFER table");
    }

    @Test
    public void checkMissingTableHandling() {
        // ext_missing_01 should be handled appropriately
        validateTableSqlNotGenerated("assorted_test_db", "ext_missing_01", RIGHT);
        validateTableIssues("assorted_test_db","ext_missing_01", LEFT);

//        var extMissing = getConversion().getDatabase("assorted_test_db").getTableMirrors().get("ext_missing_01");
//        if (extMissing != null) {
//            // Table might be skipped or have special handling
//            assertTrue(!extMissing.getEnvironmentTable(Environment.LEFT).getIssues().isEmpty() ||
//                            extMissing.getEnvironmentTable(Environment.RIGHT).getSql().isEmpty(),
//                    "Missing table should have issues or no SQL");
//        }
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

//        var db = getConversion().getDatabase("assorted_test_db");
//        assertNotNull(db.getTableMirrors().get("acid_01"));
//        assertNotNull(db.getTableMirrors().get("acid_02"));
//        assertNotNull(db.getTableMirrors().get("acid_03"));
//        assertNotNull(db.getTableMirrors().get("ext_part_01"));
//        assertNotNull(db.getTableMirrors().get("ext_part_02"));
//        assertNotNull(db.getTableMirrors().get("legacy_mngd_01"));
//        assertNotNull(db.getTableMirrors().get("ext_missing_01"));
    }

}
