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

import com.cloudera.utils.hms.mirror.MessageCode;
import com.cloudera.utils.hms.mirror.PhaseState;
import com.cloudera.utils.hms.mirror.cli.Mirror;
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
                "--hms-mirror.config.data-strategy=STORAGE_MIGRATION",
                "--hms-mirror.config.target-namespace=ofs://OHOME90",
                "--hms-mirror.config.warehouse-plans=ext_purge_odd_parts=/finance/external-fso:/finance/managed-fso",
//                "--hms-mirror.config.warehouse-directory=/finance/managed-fso",
//                "--hms-mirror.config.external-warehouse-directory=/finance/external-fso",
//                "--hms-mirror.config.evaluate-partition-location=true",
                "--hms-mirror.config.distcp=PULL",
                "--hms-mirror.config.filename=/config/default.yaml.cdp",
                "--hms-mirror.config.storage-migration-strict=true",
                "--hms-mirror.conversion.test-filename=/test_data/ext_purge_odd_parts_01.yaml",
                "--hms-mirror.config.output-dir=${user.home}/.hms-mirror/test-output/e2e/cdp/sm_wd_epl_rdl_dc_odd_strict"
        }
)
@Slf4j
/*
STORAGE_MIGRATION test used to show how to move data from one directory to another, within the same namespace.

Since the -smn is not specified, the namespace is assumed to be the same as the original table location.
The -wd and -ewd are used to define the warehouse directories.  The -epl is used to evaluate the partition locations and
with -rdl, the default location is reset to the new warehouse directory.

There should be no issue now that the default location is reset to the new warehouse directory.

 */
public class Test_sm_wd_epl_rdl_dc_odd_strict extends E2EBaseTest {

    @Test
    public void issueCountTest() {
        validateTableIssueCount("ext_purge_odd_parts", "web_sales", Environment.LEFT, 1);
    }

    @Test
    public void errorTest() {
        validateTableErrorCount("ext_purge_odd_parts", "web_sales",
                Environment.LEFT, 1);
    }

    @Test
    public void phaseTest() {
        validatePhase("ext_purge_odd_parts", "web_sales", PhaseState.ERROR);
    }

    @Test
    public void returnCodeTest() {
        // Get Runtime Return Code.
        long rtn = getReturnCode();

        // Has non-standard partition locations which can't be translated without additional
        // GLM entries.

        // Verify the return code.
        assertEquals(1L, rtn, "Return Code Failure: " + rtn);
    }

    @Test
    public void warningCodeTest() {
        // Get Runtime Return Code.
        long actual = getWarningCode();
        // Verify the return code.
        long expected = getCheckCode(
                MessageCode.ALIGNED_DISTCP_EXECUTE,
                MessageCode.IGNORING_TBL_FILTERS_W_TEST_DATA,
//                MessageCode.RDL_DC_WARNING_TABLE_ALIGNMENT,
//                MessageCode.STORAGE_MIGRATION_NAMESPACE_LEFT,
                MessageCode.DISTCP_W_TABLE_FILTERS
        );

        assertEquals(expected, actual, "Warning Code Failure: ");


    }

}
