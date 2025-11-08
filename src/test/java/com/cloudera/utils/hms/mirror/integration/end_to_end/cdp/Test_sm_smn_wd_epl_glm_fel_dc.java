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

package com.cloudera.utils.hms.mirror.integration.end_to_end.cdp;


import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.PhaseState;
import com.cloudera.utils.hms.mirror.cli.Mirror;
import com.cloudera.utils.hms.mirror.integration.end_to_end.E2EBaseTest;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = Mirror.class,
        args = {
                "--hms-mirror.config.force-external-location=true",
                "--hms-mirror.config.output-dir=${user.home}/.hms-mirror/test-output/e2e/cdp/sm_smn_wd_epl_glm_fel_dc",
                "--hms-mirror.config.storage-migration-strict=false",
                "--hms-mirror.conversion.test-filename=/test_data/ext_purge_odd_parts_01.yaml",
                "--hms-mirror.config.global-location-map=/user/dstreev/datasets/alt-locations/load_web_sales=/finance/external-fso/load_web_sales,/warehouse/tablespace/external/hive=/finance/external-fso"

        })
@ActiveProfiles("e2e-cdp-sm_smn_wd_epl_dc")
@Slf4j
/*
Issues: Need to post warning when table/partition(s) new location isn't in the -[e]wd location.
 */
public class Test_sm_smn_wd_epl_glm_fel_dc extends E2EBaseTest {

    @Test
    public void phaseTest() {
        validatePhase("ext_purge_odd_parts", "web_sales", PhaseState.PROCESSED);
    }

    @Test
    public void returnCodeTest() {
        // Get Runtime Return Code.
        long rtn = getReturnCode();

        // Non-standard locations can't be migrated without additional GLM entries.

        // Verify the return code.
        long check = 0L;
        assertEquals(check, rtn, "Return Code Failure: " + rtn);
    }

    @Test
    public void validateTableLeftIssueCount() {
        validateTableIssueCount("ext_purge_odd_parts", "web_sales",
                Environment.LEFT, 2);
    }

    @Test
    public void validateTableLeftErrorCount() {
        validateTableErrorCount("ext_purge_odd_parts", "web_sales",
                Environment.LEFT, 1);
    }

    @Test
    public void validateTableRightIssueCount() {
        validateTableIssueCount("ext_purge_odd_parts", "web_sales",
                Environment.RIGHT, 18);
    }

    @Test
    public void validateTableRightErrorCount() {
        validateTableErrorCount("ext_purge_odd_parts", "web_sales",
                Environment.RIGHT, 0);
    }


}
