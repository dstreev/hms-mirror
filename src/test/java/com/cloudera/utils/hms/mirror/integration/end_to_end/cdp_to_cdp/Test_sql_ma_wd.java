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
import com.cloudera.utils.hms.mirror.domain.core.DBMirror;
import com.cloudera.utils.hms.mirror.domain.core.TableMirror;
import com.cloudera.utils.hms.mirror.domain.dto.ConnectionDto;
import com.cloudera.utils.hms.mirror.domain.support.ConversionResult;
import com.cloudera.utils.hms.mirror.integration.end_to_end.E2EBaseTest;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.cloudera.utils.hms.mirror.PhaseState;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.util.TableUtils;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = Mirror.class,
        args = {
                "--hms-mirror.config.data-strategy=SQL",
                "--hms-mirror.config.migrate-acid=true",
                "--hms-mirror.config.warehouse-directory=/warehouse/managed",
                "--hms-mirror.config.external-warehouse-directory=/warehouse/external",
                "--hms-mirror.conversion.test-filename=/test_data/assorted_tbls_01.yaml",
                "--hms-mirror.config.filename=/config/default.yaml.cdp-cdp",
                "--hms-mirror.config.output-dir=${user.home}/.hms-mirror/test-output/e2e/cdp_cdp/sql_ma_wd"
        })
@Slf4j
public class Test_sql_ma_wd extends E2EBaseTest {
    //        String[] args = new String[]{"-d", "SQL",
//                "-ma",
//                "-wd", "/warehouse/managed", "-ewd", "/warehouse/external",
//                "-ltd", ASSORTED_TBLS_04,
//                "-cfg", CDP_CDP,
//                "-o", outputDir
//        };
//
//        long rtn = 0;
//        MirrorLegacy mirror = new MirrorLegacy();
//        rtn = mirror.go(args);
//        int check = 0;
//        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, rtn, check);

    @Test
    public void returnCodeTest() {
        // Get Runtime Return Code.
        long rtn = getReturnCode();
        // Verify the return code.
        long check = 0L;
        assertEquals(check * -1, rtn, "Return Code Failure: " + rtn);
    }

    @Test
    public void statisticsValidationTest() {
        // Validate operation statistics based on test output
        DBMirror dbMirror = getDBMirrorOrFail("assorted_test_db");
//        assertNotNull(getConversion().getDatabase("assorted_test_db"), "Database should exist");
        assertEquals(7,
                getTableCount("assorted_test_db"),
                "Should have 7 tables processed with migrate-acid=true");
    }

    @Test
    public void phaseValidationTest() {
        getDBMirrorOrFail("assorted_test_db");
        getTableMirrorOrFail("assorted_test_db", "acid_01");
//        validatePhase("assorted_test_db", "acid_01", PhaseState.CALCULATED_SQL);
        // Validate phase state from test output - SQL strategy reaches CALCULATED_SQL
//        assertNotNull(getConversion().getDatabase("assorted_test_db"), "Database must exist");
//        assertTrue(getConversion().getDatabase("assorted_test_db").getTableMirrors().size() > 0,
//                "Must have tables to validate phases");

//        String firstTable = getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().keySet().iterator().next();
        validatePhase("assorted_test_db", "acid_01", PhaseState.CALCULATED_SQL);

    }

    @Test
    public void acidTableValidationTest() {
        // Validate ACID tables are properly identified
        validateTableIsACID("assorted_test_db", "acid_01", Environment.LEFT);
        validateTableIsACID("assorted_test_db", "acid_02", Environment.LEFT);
        validateTableIsACID("assorted_test_db", "acid_03", Environment.LEFT);
    }

    @Test
    public void warehouseDirectoryValidationTest() {
        // Validate warehouse directories are set correctly for managed tables
        ConversionResult conversionResult = getConversion();
        ConnectionDto connection = conversionResult.getConnection(Environment.RIGHT);
        assertEquals("hdfs://HOME90", connection.getHcfsNamespace());
    }

    @Test
    public void sqlPairValidationTest() {
        // Validate SQL pairs for ACID table migration
        TableMirror tableMirror = getTableMirrorOrFail("assorted_test_db", "acid_01");
//        if (getConversion().getDatabase("assorted_test_db").getTableMirrors().containsKey("acid_01")) {
        // SQL strategy uses shadow tables for ACID migration
        validateTableSqlPair("assorted_test_db", Environment.RIGHT, "acid_01",
                "Creating Shadow Table",
                "CREATE EXTERNAL TABLE `hms_mirror_shadow_acid_01`(\n" +
                        "`id` string,\n" +
                        "`checkvalue` string)\n" +
                        "ROW FORMAT SERDE\n" +
                        "'org.apache.hadoop.hive.ql.io.orc.OrcSerde'\n" +
                        "STORED AS INPUTFORMAT\n" +
                        "'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'\n" +
                        "OUTPUTFORMAT\n" +
                        "'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'\n" +
                        "LOCATION\n" +
                        "'hdfs://HDP50/apps/hive/warehouse/export_assorted_test_db/acid_01'\n" +
                        "TBLPROPERTIES (\n" +
                        "'hms-mirror_shadow_table'='true',\n" +
                        "'hms-mirror_transfer_table'='true'\n" +
                        ")");

        validateTableSqlPair("assorted_test_db", Environment.RIGHT, "acid_01",
                "Moving data to transfer table",
                "FROM hms_mirror_shadow_acid_01 INSERT OVERWRITE TABLE acid_01 SELECT *");
//        }
    }

    @Test
    public void tableIssueValidationTest() {
        // Validate issues for ACID tables
        TableMirror tableMirror = getTableMirrorOrFail("assorted_test_db", "acid_01");
//        if (getConversion().getDatabase("assorted_test_db").getTableMirrors().containsKey("acid_01")) {
        // ACID tables should have expected issues about location stripping
        validateTableIssues("assorted_test_db", "acid_01", Environment.RIGHT);
//        assertTrue(!tableMirror.getEnvironmentTable(Environment.RIGHT)
//                        .getIssues().isEmpty(),
//                "ACID tables should have issues documented");
//        }

        // External tables should have no errors
//        if (getConversion().getDatabase("assorted_test_db").getTableMirrors().containsKey("ext_part_01")) {
        validateTableIssueCount("assorted_test_db", "ext_part_01", Environment.RIGHT, 3);
//        }
    }

    @Test
    public void partitionValidationTest() {
        // Validate partitioned tables
        TableMirror tableMirror = getTableMirrorOrFail("assorted_test_db", "ext_part_01");
        validatePartitioned("assorted_test_db", "ext_part_01", Environment.LEFT);
        validatePartitioned("assorted_test_db", "ext_part_02", Environment.LEFT);
//        if (getConversion().getDatabase("assorted_test_db").getTableMirrors().containsKey("ext_part_01")) {
//            // ext_part_01 has partitions based on test data
//            assertTrue(getConversion().getDatabase("assorted_test_db")
//                            .getTableMirrors().get("ext_part_01")
//                            .getEnvironmentTable(Environment.LEFT)
//                            .getPartitioned(),
//                    "ext_part_01 should be partitioned");
//        }
//
//        if (getConversion().getDatabase("assorted_test_db").getTableMirrors().containsKey("ext_part_02")) {
//            assertFalse(getConversion().getDatabase("assorted_test_db")
//                            .getTableMirrors().get("ext_part_02")
//                            .getEnvironmentTable(Environment.LEFT)
//                            .getPartitioned(),
//                    "ext_part_02 shouldn't be partitioned");
//        }
    }

}
