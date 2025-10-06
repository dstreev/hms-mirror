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
    //        String[] args = new String[]{"-d", "EXPORT_IMPORT",
//                "-ma", "-wd", "/warehouse/managed", "-ewd", "/warehouse/external",
//                "-ep", "500",
//                "-ltd", ASSORTED_TBLS_04,
//                "-cfg", CDP_CDP,
//                "-o", outputDir
//        };
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
    public void databaseLocationTest() {
        // Validate database location on RIGHT with external warehouse directory
        validateDBLocation("assorted_test_db", Environment.RIGHT, 
                "hdfs://HOME90/warehouse/tablespace/external/hive/assorted_test_db.db");
    }

    @Test
    public void acid_01_phaseTest() {
        // Validate phase state for acid_01 ACID table
        validatePhase("assorted_test_db", "acid_01", PhaseState.CALCULATED_SQL);
    }

    @Test
    public void acid_02_phaseTest() {
        // Validate phase state for acid_02 ACID table
        validatePhase("assorted_test_db", "acid_02", PhaseState.CALCULATED_SQL);
    }

    @Test
    public void acid_03_phaseTest() {
        // Validate phase state for acid_03 partitioned ACID table
        validatePhase("assorted_test_db", "acid_03", PhaseState.CALCULATED_SQL);
    }

    @Test
    public void ext_part_01_phaseTest() {
        // Validate phase state for ext_part_01 external partitioned table
        validatePhase("assorted_test_db", "ext_part_01", PhaseState.CALCULATED_SQL);
    }

    @Test
    public void ext_part_02_phaseTest() {
        // Validate phase state for ext_part_02 external partitioned table
        validatePhase("assorted_test_db", "ext_part_02", PhaseState.CALCULATED_SQL);
    }

    @Test
    public void legacy_mngd_01_phaseTest() {
        // Validate phase state for legacy_mngd_01 legacy managed table
        validatePhase("assorted_test_db", "legacy_mngd_01", PhaseState.CALCULATED_SQL);
    }

    @Test
    public void ext_missing_01_phaseTest() {
        // Validate phase state for ext_missing_01 external table
        validatePhase("assorted_test_db", "ext_missing_01", PhaseState.CALCULATED_SQL);
    }
    
    @Test
    public void validateExtMissing01Issue() {
        // ext_missing_01 exists on RIGHT but not on LEFT, should have specific issue
        validateTableIssueCount("assorted_test_db", "ext_missing_01", Environment.RIGHT, 1);
        var issues = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("ext_missing_01").getEnvironmentTable(Environment.RIGHT).getIssues();
        assertTrue(issues.contains("Schema exists on the target, but not on the source."));
    }

    @Test
    public void validateTableCount() {
        // Validate that we have 7 tables in the database
        assertEquals(7, getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().size(), "Table count mismatch");
    }

    @Test
    public void validateExportImportStrategy() {
        // Validate that all tables use EXPORT_IMPORT strategy
        assertEquals("EXPORT_IMPORT", getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_01").getStrategy().toString());
        assertEquals("EXPORT_IMPORT", getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_02").getStrategy().toString());
        assertEquals("EXPORT_IMPORT", getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_03").getStrategy().toString());
        assertEquals("EXPORT_IMPORT", getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("ext_part_01").getStrategy().toString());
        assertEquals("EXPORT_IMPORT", getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("ext_part_02").getStrategy().toString());
        assertEquals("EXPORT_IMPORT", getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("legacy_mngd_01").getStrategy().toString());
        assertEquals("EXPORT_IMPORT", getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("ext_missing_01").getStrategy().toString());
    }

    @Test
    public void validateACIDTableIsTransactional() {
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
    public void validateExportPaths() {
        // Validate export paths for tables
        var acid01LeftSql = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.LEFT).getSql();
        boolean foundExport = false;
        for (var pair : acid01LeftSql) {
            if (pair.getDescription().equals("EXPORT Table")) {
                assertEquals("EXPORT TABLE acid_01 TO \"hdfs://HDP50/apps/hive/warehouse/export_assorted_test_db/acid_01\"",
                        pair.getAction());
                foundExport = true;
            }
        }
        assertTrue(foundExport, "EXPORT statement not found for acid_01");
    }

    @Test
    public void validateImportPaths() {
        // Validate import paths for tables
        var acid01RightSql = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.RIGHT).getSql();
        boolean foundImport = false;
        for (var pair : acid01RightSql) {
            if (pair.getDescription().equals("IMPORT Table")) {
                assertEquals("IMPORT TABLE acid_01 FROM \"hdfs://HDP50/apps/hive/warehouse/export_assorted_test_db/acid_01\"",
                        pair.getAction());
                foundImport = true;
            }
        }
        assertTrue(foundImport, "IMPORT statement not found for acid_01");
    }

    @Test
    public void validateExternalTableImport() {
        // Validate IMPORT command for external table includes EXTERNAL and LOCATION
        var extPart01RightSql = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("ext_part_01").getEnvironmentTable(Environment.RIGHT).getSql();
        boolean foundImport = false;
        for (var pair : extPart01RightSql) {
            if (pair.getDescription().equals("IMPORT Table")) {
                assertEquals("IMPORT EXTERNAL TABLE ext_part_01 FROM \"hdfs://HDP50/apps/hive/warehouse/export_assorted_test_db/ext_part_01\" LOCATION \"hdfs://HOME90/warehouse/tablespace/external/hive/assorted_test_db.db/ext_part_01\"",
                        pair.getAction());
                foundImport = true;
            }
        }
        assertTrue(foundImport, "IMPORT EXTERNAL statement not found for ext_part_01");
    }

    @Test
    public void validatePartitionCount() {
        // Validate partition count for ext_part_01
        validatePartitionCount("assorted_test_db", "ext_part_01", Environment.LEFT, 440);
        
        // ext_part_02 has no partitions based on the test data
        validatePartitionCount("assorted_test_db", "ext_part_02", Environment.LEFT, 0);
        
        // Validate partition count for acid_03
        validatePartitionCount("assorted_test_db", "acid_03", Environment.LEFT, 200);
    }

    @Test
    public void validateTableLocations() {
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
    public void validateBucketingForACIDTables() {
        // Validate acid_01 has 2 buckets
        var acid01LeftDef = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.LEFT).getDefinition();
        assertTrue(acid01LeftDef.stream().anyMatch(line -> line.contains("INTO 2 BUCKETS")));
        
        // acid_02 doesn't have buckets defined
        // Validate acid_03 has 6 buckets
        var acid03LeftDef = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_03").getEnvironmentTable(Environment.LEFT).getDefinition();
        assertTrue(acid03LeftDef.stream().anyMatch(line -> line.contains("INTO 6 BUCKETS")));
    }

    @Test
    public void validateTableProperties() {
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
    public void validateDatabaseSql() {
        // Validate database creation SQL on RIGHT
        assertTrue(validateDBSqlPair("assorted_test_db", Environment.RIGHT,
                "Create Database",
                "CREATE DATABASE IF NOT EXISTS assorted_test_db\n"));
        assertTrue(validateDBSqlPair("assorted_test_db", Environment.RIGHT,
                "Alter Database Location",
                "ALTER DATABASE assorted_test_db SET LOCATION \"hdfs://HOME90/warehouse/tablespace/external/hive/assorted_test_db.db\""));
    }

    @Test
    public void validateTableExistence() {
        // Validate that most tables exist on LEFT side
        assertTrue(getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.LEFT).isExists());
        assertTrue(getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_02").getEnvironmentTable(Environment.LEFT).isExists());
        assertTrue(getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_03").getEnvironmentTable(Environment.LEFT).isExists());
        assertTrue(getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("ext_part_01").getEnvironmentTable(Environment.LEFT).isExists());
        assertTrue(getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("ext_part_02").getEnvironmentTable(Environment.LEFT).isExists());
        assertTrue(getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("legacy_mngd_01").getEnvironmentTable(Environment.LEFT).isExists());
        
        // ext_missing_01 doesn't exist on LEFT but exists on RIGHT
        assertFalse(getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("ext_missing_01").getEnvironmentTable(Environment.LEFT).isExists());
        assertTrue(getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("ext_missing_01").getEnvironmentTable(Environment.RIGHT).isExists());
    }

    @Test
    public void validateTotalPhaseCount() {
        // Validate total phase count for tables
        assertEquals(6, getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_01").getTotalPhaseCount().get(), 
                "acid_01 should have 6 total phases");
        assertEquals(6, getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("ext_part_01").getTotalPhaseCount().get(), 
                "ext_part_01 should have 6 total phases");
    }

    @Test
    public void validatePhaseSummary() {
        // Validate phase summary shows all tables in CALCULATED_SQL phase
        var phaseSummary = getConversion().getDatabase("assorted_test_db").getPhaseSummary();
        assertNotNull(phaseSummary);
        assertEquals(7, phaseSummary.get(PhaseState.CALCULATED_SQL).intValue(), 
                "Should have 7 tables in CALCULATED_SQL phase");
    }

}
