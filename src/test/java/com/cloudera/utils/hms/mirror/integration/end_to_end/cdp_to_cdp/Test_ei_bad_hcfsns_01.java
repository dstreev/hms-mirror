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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = Mirror.class,
        args = {
                "--hms-mirror.config.data-strategy=EXPORT_IMPORT",
                "--hms-mirror.conversion.test-filename=/test_data/assorted_tbls_01.yaml",
                "--hms-mirror.config.filename=/config/default.yaml.cdp-cdp.bad.hcfsns",
                "--hms-mirror.config.output-dir=${user.home}/.hms-mirror/test-output/e2e/cdp_cdp/ei_bad_hcfsns_01"
        })
@Slf4j
public class Test_ei_bad_hcfsns_01 extends E2EBaseTest {
//
//        String[] args = new String[]{"-d", "EXPORT_IMPORT",
//                "-sql", "-ltd", ASSORTED_TBLS_04,
//                "-cfg", CDP_CDP_BNS,
//                "-o", outputDir
//        };
//
//        long rtn = 0;
//        MirrorLegacy mirror = new MirrorLegacy();
//        rtn = mirror.go(args);
//        int check = 3;
//        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);

    @Test
    public void returnCodeTest() {
        // Get Runtime Return Code.
        long rtn = getReturnCode();
        // Verify the return code.
        long check = 1L;
        assertEquals(check, rtn, "Return Code Failure: " + rtn);
    }

    @Test
    public void statisticsValidationTest() {
        // Validate operation statistics based on test output
        validateTableCount("assorted_test_db", 4);
//        assertNotNull(getConversion().getDatabase("assorted_test_db"), "Database should exist");
//        assertEquals(4,
//                getConversion().getDatabase("assorted_test_db").getTableMirrors().size(),
//                "Should have 4 tables processed with bad hcfsns configuration");
    }
    
    @Test
    public void phaseValidationTest() {
        // Validate phase state from test output
//        assertNotNull(getConversion().getDatabase("assorted_test_db"), "Database must exist");
//        assertTrue(getConversion().getDatabase("assorted_test_db").getTableMirrors().size() > 0,
//                "Must have tables to validate phases");


//        String firstTable = getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().keySet().iterator().next();
        Map<String, TableMirror> TableMap = null;
        try {
            TableMap = getTableMirrorRepository()
                    .findByDatabase(getConversion().getKey(), "assorted_test_db");
        } catch (RepositoryException e) {
            throw new RuntimeException(e);
        }
        String firstTable = TableMap.keySet().iterator().next();
        validatePhase("assorted_test_db", firstTable, PhaseState.ERROR);
    }
    
    @Test
    public void exportImportStrategyValidationTest() {
        // Validate EXPORT_IMPORT strategy is being used;
        assertEquals(DataStrategyEnum.EXPORT_IMPORT,
                getConversion().getJob().getStrategy(),
                "Data strategy should be EXPORT_IMPORT");
    }

}
