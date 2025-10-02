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
import com.cloudera.utils.hms.mirror.integration.end_to_end.E2EBaseTest;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.cloudera.utils.hms.mirror.PhaseState;
import com.cloudera.utils.hms.mirror.domain.support.Environment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = Mirror.class,
        args = {
                "--hms-mirror.config.data-strategy=HYBRID",
                "--hms-mirror.config.migrate-acid-only=true",
                "--hms-mirror.config.downgrade-acid=true",
                "--hms-mirror.config.target-namespace=s3a://my_cs_bucket",
                "--hms-mirror.conversion.test-filename=/test_data/assorted_tbls_01.yaml",
                "--hms-mirror.config.filename=/config/default.yaml.cdp-cdp",
                "--hms-mirror.config.output-dir=${user.home}/.hms-mirror/test-output/e2e/cdp_cdp/hybrid_mao_da_cs"
        })
@Slf4j
public class Test_hybrid_mao_da_cs extends E2EBaseTest {
    //        String[] args = new String[]{"-d", "HYBRID",
//                "-mao", "-da", "-cs", TARGET_NAMESPACE,
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
        assertEquals(check, rtn, "Return Code Failure: " + rtn);
    }

    @Test
    public void statisticsValidationTest() {
        // Validate operation statistics based on test output
        assertNotNull(getConversion().getDatabase("assorted_test_db"), "Database should exist");
        assertEquals(3, 
                getConversion().getDatabase("assorted_test_db").getTableMirrors().size(),
                "Should have 3 ACID tables processed with migrate-acid-only");
    }
    
    @Test
    public void phaseValidationTest() {
        // Validate phase state from test output
        assertNotNull(getConversion().getDatabase("assorted_test_db"), "Database must exist");
        assertTrue(getConversion().getDatabase("assorted_test_db").getTableMirrors().size() > 0,
                "Must have tables to validate phases");
        
        validatePhase("assorted_test_db", "acid_01", PhaseState.CALCULATED_SQL);
        validatePhase("assorted_test_db", "acid_02", PhaseState.CALCULATED_SQL);
        validatePhase("assorted_test_db", "acid_03", PhaseState.CALCULATED_SQL);
    }
    
    @Test
    public void acidTableValidationTest() {
        // Validate ACID tables are properly identified and downgraded
        validateTableIsACID("assorted_test_db", "acid_01", Environment.LEFT);
        validateTableIsACID("assorted_test_db", "acid_02", Environment.LEFT);
        validateTableIsACID("assorted_test_db", "acid_03", Environment.LEFT);
    }
    
    @Test
    public void targetNamespaceValidationTest() {
        // Validate target namespace is set correctly for common storage
        assertEquals("s3a://my_cs_bucket", 
                getConfig().getTransfer().getTargetNamespace(),
                "Target namespace should be set for common storage");
    }

}
