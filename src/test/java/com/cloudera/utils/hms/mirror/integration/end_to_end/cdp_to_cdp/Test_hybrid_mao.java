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
import com.cloudera.utils.hms.mirror.PhaseState;
import com.cloudera.utils.hms.mirror.domain.core.DBMirror;
import com.cloudera.utils.hms.mirror.domain.core.TableMirror;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.integration.end_to_end.E2EBaseTest;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = Mirror.class,
        args = {
                "--hms-mirror.config.data-strategy=HYBRID",
                "--hms-mirror.config.migrate-acid-only=true",
                "--hms-mirror.config.downgrade-acid=true",
                "--hms-mirror.conversion.test-filename=/test_data/assorted_tbls_01.yaml",
                "--hms-mirror.config.filename=/config/default.yaml.cdp-cdp",
                "--hms-mirror.config.output-dir=${user.home}/.hms-mirror/test-output/e2e/cdp_cdp/hybrid_mao"
        })
@Slf4j
public class Test_hybrid_mao extends E2EBaseTest {

    @Test
    public void returnCodeTest() {
        // Get Runtime Return Code.
        long rtn = getReturnCode();
        // Verify the return code.
        long check = 0L;
        assertEquals(check * -1, rtn, "Return Code Failure: " + rtn);
    }

    @Test
    public void databaseValidationTest() {
        // Validate database was processed
        DBMirror dbMirror = getDBMirrorOrFail("assorted_test_db");

        validateTableCount("assorted_test_db", 3);

        TableMirror tableMirror = getTableMirrorOrFail("assorted_test_db", "acid_01");
        TableMirror tableMirror2 = getTableMirrorOrFail("assorted_test_db", "acid_02");
        TableMirror tableMirror3 = getTableMirrorOrFail("assorted_test_db", "acid_03");
    }
    
    @Test
    public void checkPhaseTest() {
        // Based on actual test output, all tables reach CALCULATED_SQL state
        validatePhase("assorted_test_db", "acid_01", PhaseState.PROCESSED);
        validatePhase("assorted_test_db", "acid_02", PhaseState.PROCESSED);
        validatePhase("assorted_test_db", "acid_03", PhaseState.PROCESSED);
    }
    
    @Test
    public void acidTableValidationTest() {
        // Validate ACID tables are properly identified
        validateTableIsACID("assorted_test_db", "acid_01", Environment.LEFT);
        validateTableIsACID("assorted_test_db", "acid_02", Environment.LEFT);
        validateTableIsACID("assorted_test_db", "acid_03", Environment.LEFT);
    }
    
    @Test
    public void tableLocationValidationTest() {
        // For HYBRID with migrate-acid-only and downgrade-acid:

        // acid_03 has location in TRANSFER and SHADOW environments
        validateTableLocation("assorted_test_db", "acid_03", Environment.TRANSFER,
                "hdfs://HDP50/apps/hive/warehouse/export_assorted_test_db/acid_03");
        validateTableLocation("assorted_test_db", "acid_03", Environment.SHADOW,
                "hdfs://HDP50/apps/hive/warehouse/export_assorted_test_db/acid_03");
    }
    
    @Test
    public void tableIssueAndErrorValidationTest() {
        // Validate that migrate-acid-only filtered out non-ACID tables properly
        validateTableCount("assorted_test_db", 3);

        // Check that we don't have non-ACID tables
        validateTableNotInDatabase( "assorted_test_db", "ext_part_01");

        // Validate issues per table based on actual test output:
        // acid_01: 0 issues in LEFT (EXPORT_IMPORT)
        validateTableIssueCount("assorted_test_db", "acid_01", Environment.LEFT, 0);
        
        // acid_02: 0 issues in LEFT (EXPORT_IMPORT)  
        validateTableIssueCount("assorted_test_db", "acid_02", Environment.LEFT, 0);
        
        // acid_03: 3 issues in LEFT (partition limit exceeded message + stats settings)
        validateTableIssueCount("assorted_test_db", "acid_03", Environment.LEFT, 3);
        
        // acid_03: 3 issues in RIGHT (location stripped + stats settings)
        validateTableIssueCount("assorted_test_db", "acid_03", Environment.RIGHT, 2);
        
        // No errors for any table
        validateTableErrorCount("assorted_test_db", "acid_01", Environment.LEFT, 0);
        validateTableErrorCount("assorted_test_db", "acid_02", Environment.LEFT, 0);
        validateTableErrorCount("assorted_test_db", "acid_03", Environment.LEFT, 0);
    }
    
    @Test
    public void sqlGenerationValidationTest() {
        // Based on actual output:
        // acid_01 and acid_02 use EXPORT_IMPORT strategy with SQL in LEFT and RIGHT
        // acid_03 uses ACID strategy with SQL in LEFT and RIGHT
        
        // acid_01: EXPORT in LEFT, IMPORT in RIGHT
        validateTableSqlPair("assorted_test_db", Environment.LEFT, "acid_01", "EXPORT Table",
                "EXPORT TABLE acid_01 TO \"hdfs://HDP50/apps/hive/warehouse/export_assorted_test_db/acid_01\"");
        validateTableSqlPair("assorted_test_db", Environment.RIGHT, "acid_01", "IMPORT Table",
                "IMPORT EXTERNAL TABLE acid_01 FROM \"hdfs://HDP50/apps/hive/warehouse/export_assorted_test_db/acid_01\"");
        
        // acid_02: EXPORT in LEFT, IMPORT in RIGHT
        validateTableSqlPair("assorted_test_db", Environment.LEFT, "acid_02", "EXPORT Table",
                "EXPORT TABLE acid_02 TO \"hdfs://HDP50/apps/hive/warehouse/export_assorted_test_db/acid_02\"");
        validateTableSqlPair("assorted_test_db", Environment.RIGHT, "acid_02", "IMPORT Table",
                "IMPORT EXTERNAL TABLE acid_02 FROM \"hdfs://HDP50/apps/hive/warehouse/export_assorted_test_db/acid_02\"");
        
        // acid_03: Uses shadow table approach due to 200 partitions exceeding limit
        validateTableSqlPair("assorted_test_db", Environment.RIGHT, "acid_03", "Creating Shadow Table",
                "CREATE EXTERNAL TABLE `hms_mirror_shadow_acid_03`(\n" +
                        "`id` string,\n" +
                        "`checkvalue` string)\n" +
                        "PARTITIONED BY (\n" +
                        "`num` string)\n" +
                        "CLUSTERED BY (\n" +
                        "id)\n" +
                        "INTO 6 BUCKETS\n" +
                        "ROW FORMAT SERDE\n" +
                        "'org.apache.hadoop.hive.ql.io.orc.OrcSerde'\n" +
                        "STORED AS INPUTFORMAT\n" +
                        "'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'\n" +
                        "OUTPUTFORMAT\n" +
                        "'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'\n" +
                        "LOCATION\n" +
                        "'hdfs://HDP50/apps/hive/warehouse/export_assorted_test_db/acid_03'\n" +
                        "TBLPROPERTIES (\n" +
                        "'hms-mirror_shadow_table'='true',\n" +
                        "'downgraded_from_acid'='true',\n" +
                        "'hms-mirror_transfer_table'='true'\n" +
                        ")");
        validateTableSqlPair("assorted_test_db", Environment.RIGHT, "acid_03", 
                "Moving data to partitioned (200) transfer table",
                "FROM hms_mirror_shadow_acid_03 INSERT OVERWRITE TABLE acid_03 PARTITION (`num`) SELECT * DISTRIBUTE BY `num`");
    }

}
