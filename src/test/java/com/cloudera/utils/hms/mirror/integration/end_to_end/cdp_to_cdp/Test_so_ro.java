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
import com.cloudera.utils.hms.mirror.exceptions.RepositoryException;
import com.cloudera.utils.hms.mirror.integration.end_to_end.E2EBaseTest;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.cloudera.utils.hms.mirror.PhaseState;
import com.cloudera.utils.hms.mirror.domain.support.Environment;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = Mirror.class,
        args = {
                "--hms-mirror.config.read-only=true",
                "--hms-mirror.conversion.test-filename=/test_data/assorted_tbls_01.yaml",
                "--hms-mirror.config.filename=/config/default.yaml.cdp-cdp",
                "--hms-mirror.config.output-dir=${user.home}/.hms-mirror/test-output/e2e/cdp_cdp/so_ro"
        })
@Slf4j
public class Test_so_ro extends E2EBaseTest {
    //        String[] args = new String[]{
//                "-ro",
//                "-ltd", ASSORTED_TBLS_04,
//                "-cfg", CDP_CDP,
//                "-o", outputDir
//        };
//
//        long rtn = 0;
//        MirrorLegacy mirror = new MirrorLegacy();
//        rtn = mirror.go(args);
//        long check = 0;
//        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);

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
        validateTableCount("assorted_test_db", 4);
    }

    @Test
    public void phaseValidationTest() {
        // Validate phase state from test output - read-only reaches CALCULATED_SQL

        Map<String, TableMirror> TableMap = null;
        try {
            TableMap = getTableMirrorRepository().findByDatabase(getConversion().getKey(), "assorted_test_db");
        } catch (RepositoryException e) {
            throw new RuntimeException(e);
        }
        String firstTable = TableMap.keySet().iterator().next();
        validatePhase("assorted_test_db", firstTable, PhaseState.CALCULATED_SQL);
    }

    @Test
    public void readOnlyModeValidationTest() {
        // In read-only mode, RIGHT environment should not have SQL generated
//        assertNotNull(getConversion().getDatabase("assorted_test_db").getTableMirrors().get("acid_01"),
//                "acid_01 should be discovered");
        // Validate ACID tables are identified
        validateTableIsACID("assorted_test_db", "acid_01", Environment.LEFT);
        validateTableIsACID("assorted_test_db", "acid_02", Environment.LEFT);
        validateTableIsACID("assorted_test_db", "acid_03", Environment.LEFT);
    }

    @Test
    public void tableIssueValidationTest() {
        // Validate no errors in read-only mode
        validateTableIssueCount("assorted_test_db", "acid_01", Environment.LEFT, 0);
        validateTableIssueCount("assorted_test_db", "acid_02", Environment.LEFT, 0);
        validateTableIssueCount("assorted_test_db", "acid_03", Environment.LEFT, 0);

        // Non-ACID tables should also have no issues in read-only
//        if (getConversion().getDatabase("assorted_test_db").getTableMirrors().containsKey("ext_part_01")) {
        validateTableIssueCount("assorted_test_db", "ext_part_01", Environment.LEFT, 0);
//        }
    }

    @Test
    public void tableLocationValidationTest() {
        // Validate table locations for discovered tables
//        if (getConversion().getDatabase("assorted_test_db").getTableMirrors().containsKey("ext_part_01")) {
            // External partitioned table should have location
            validateTableLocation("assorted_test_db", "ext_part_01", Environment.LEFT,
                    "hdfs://HDP50/warehouse/tablespace/external/hive/assorted_test_db.db/ext_part_01");
//        }
    }


}
