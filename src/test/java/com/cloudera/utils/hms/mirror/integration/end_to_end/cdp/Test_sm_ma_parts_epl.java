/*
 * Copyright (c) 2024. Cloudera, Inc. All Rights Reserved
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
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = Mirror.class,
        args = {
                "--hms-mirror.config.data-strategy=STORAGE_MIGRATION",
                "--hms-mirror.config.output-dir=${user.home}/.hms-mirror/test-output/e2e/cdp/sm_ma_parts_epl",
                "--hms-mirror.conversion.test-filename=/test_data/acid_w_parts_01.yaml",
                "--hms-mirror.config.filename=/config/default.yaml.cdp",
                "--hms-mirror.config.align-locations=true",
                "--hms-mirror.config.target-namespace=ofs://OHOME90",
                "--hms-mirror.config.migrate-acid=true",
                "--hms-mirror.config.warehouse-directory=/new/warehouse/managed",
                "--hms-mirror.config.external-warehouse-directory=/new/warehouse/external"
        })
@Slf4j
/*
STORAGE_MIGRATION test.  Defining the warehouse directories (-wd and -ewd) along with -epl (evaluation of partition locations).
We've also added -dc to this to produce a distcp plan for this data migration.
It should only evaluate non-acid tables.

In this test, the locations of the partitions doesn't line up with the warehouse directories listed.  And since we're
not using -rdl (reset default location), we issue warnings about the partitions that don't line up.

This storage migration doesn't require the creation of any new tables.  We will simply ALTER the table and partition
locations.
 */
public class Test_sm_ma_parts_epl extends E2EBaseTest {

    @Test
    public void issueTest() {
        validateTableIssueCount("assort_test_db", "acid_03",
                Environment.RIGHT, 2);
    }

    @Test
    public void phaseTest() {
        validatePhase("assort_test_db", "acid_03", PhaseState.CALCULATED_SQL);
    }

    @Test
    public void returnCodeTest() {
        // Get Runtime Return Code.
        long rtn = getReturnCode();
        // Verify the return code.
        long check = 0L;
        assertEquals(check, rtn, "Return Code Failure: " + rtn);
    }

//    @Test
//    public void sqlTest() {
//        Boolean foundAT = Boolean.FALSE;
//        Boolean foundOddPart = Boolean.FALSE;
//        Boolean foundOddPart2 = Boolean.FALSE;
//
//        for (Pair pair : getConversion().getDatabase("assort_test_db")
//                .getTableMirrors().get("acid_03")
//                .getEnvironmentTable(Environment.LEFT).getSql()) {
//            if (pair.getDescription().trim().equals("Alter Table Location")) {
//                assertEquals("Location doesn't match", "ALTER TABLE web_sales SET LOCATION \"ofs://OHOME90/warehouse/tablespace/external/hive/ext_purge_odd_parts.db/web_sales\"", pair.getAction());
//                foundAT = Boolean.TRUE;
//            }
//            if (pair.getDescription().trim().equals("Alter Table Partition Spec `ws_sold_date_sk`='2451180' Location")) {
//                assertEquals("Location doesn't match", "ALTER TABLE web_sales PARTITION " +
//                        "(`ws_sold_date_sk`='2451180') SET LOCATION \"ofs://OHOME90/warehouse/tablespace/external/hive/ext_purge_odd_parts.db/web_sales/ws_sold_date_sk=2451180\"", pair.getAction());
//                foundOddPart = Boolean.TRUE;
//            }
//            if (pair.getDescription().trim().equals("Alter Table Partition Spec `ws_sold_date_sk`='2451188' Location")) {
//                assertEquals("Location doesn't match", "ALTER TABLE web_sales PARTITION " +
//                        "(`ws_sold_date_sk`='2451188') SET LOCATION \"ofs://OHOME90/user/dstreev/datasets/alt-locations/web_sales/ws_sold_date_sk=2451188\"", pair.getAction());
//                foundOddPart2 = Boolean.TRUE;
//            }
//        }
//        assertEquals("Alter Table Location not found", Boolean.TRUE, foundAT);
//        assertEquals("Alter Odd Part Location not found", Boolean.TRUE, foundOddPart);
//        assertEquals("Alter Odd Part 2 Location not found", Boolean.TRUE, foundOddPart2);
//    }

}
