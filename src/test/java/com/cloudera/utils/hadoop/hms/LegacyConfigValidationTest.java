/*
 * Copyright 2021 Cloudera, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.utils.hadoop.hms;

import com.cloudera.utils.hadoop.hms.mirror.MessageCode;
import com.cloudera.utils.hadoop.hms.mirror.Pair;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.cloudera.utils.hadoop.hms.TestSQL.*;
import static org.junit.Assert.assertTrue;

public class LegacyConfigValidationTest extends MirrorTestBase {

    @AfterClass
    public static void tearDownClass() throws Exception {
        dataCleanup(Boolean.FALSE);
    }

    public Boolean dataSetup01() {
        if (!DataState.getInstance().isDataCreated()) {
            String nameofCurrMethod = new Throwable()
                    .getStackTrace()[0]
                    .getMethodName();

            String outputDir = outputDirBase + nameofCurrMethod;

            String[] args = new String[]{"-d", "STORAGE_MIGRATION", "-smn", "s3a://something_not_relevant",
                    "-wd", "/hello", "-ewd", "/hello-ext",
                    "-db", DataState.getInstance().getWorking_db(), "-o", outputDir,
                    "-cfg", DataState.getInstance().getConfiguration()};
            args = toExecute(args, execArgs, Boolean.TRUE);

            List<Pair> leftSql = new ArrayList<Pair>();
            build_use_db(leftSql);

            build_n_populate(CREATE_LEGACY_ACID_TBL_N_BUCKETS, "acid_01", 2, TBL_INSERT, null, leftSql);
            build_n_populate(CREATE_LEGACY_ACID_TBL_N_BUCKETS, "acid_02", 6, TBL_INSERT, null, leftSql);
            build_n_populate(CREATE_EXTERNAL_TBL_PARTITIONED, "ext_part_01", null, TBL_INSERT_PARTITIONED, null, leftSql);
            build_n_populate(CREATE_EXTERNAL_TBL, "ext_part_02", null, TBL_INSERT, null, leftSql);

            Mirror cfgMirror = new Mirror();
            long rtn = cfgMirror.setupSql(args, leftSql, null);
            DataState.getInstance().setDataCreated(Boolean.TRUE);
        }
        return Boolean.TRUE;
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        DataState.getInstance().setConfiguration(HDP2_CDP);
        dataSetup01();
    }

    @After
    public void tearDown() throws Exception {
        dataCleanup(Boolean.TRUE);
    }

    @Test
    public void test_acid_hybrid_da_cs_r_all_leg() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-d", "HYBRID", "-db", DataState.getInstance().getWorking_db(),
                "-ma", "-da", "-r", "-cs", common_storage,
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        long check = MessageCode.REPLACE_ONLY_WITH_SQL.getLong();

        assertTrue("Return Code Failure: " + rtn + " expecting: " + check, rtn == check);
    }

    @Test
    public void test_acid_hybrid_da_cs_r_leg() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-d", "HYBRID", "-db", DataState.getInstance().getWorking_db(),
                "-mao", "-da", "-r", "-cs", common_storage,
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);

        long check = MessageCode.REPLACE_ONLY_WITH_SQL.getLong();

        assertTrue("Return Code Failure: " + rtn + " expecting: " + check, rtn == check);
    }

    @Test
    public void test_acid_hybrid_da_r_leg() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-d", "HYBRID", "-db", DataState.getInstance().getWorking_db(),
                "-mao", "-da", "-r",
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);

        long check = MessageCode.REPLACE_ONLY_WITH_SQL.getLong();

        assertTrue("Return Code Failure: " + rtn + " expecting: " + check, rtn == check);
    }

    @Test
    public void test_acid_hybrid_r_leg() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-d", "HYBRID", "-db", DataState.getInstance().getWorking_db(),
                "-mao", "-r",
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);

        long check = MessageCode.REPLACE_ONLY_WITH_SQL.getLong();
        check = check | MessageCode.REPLACE_ONLY_WITH_DA.getLong();
        assertTrue("Return Code Failure: " + rtn + " expecting: " + check, rtn == check);
    }

    @Test
    public void test_acid_sql_r_leg() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL", "-db", DataState.getInstance().getWorking_db(),
                "-mao", "-r",
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        long check = MessageCode.REPLACE_ONLY_WITH_DA.getLong();
        assertTrue("Return Code Failure: " + rtn + " expecting: " + check, rtn == check);
    }

    @Test
    public void test_so_ro_leg() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-db", DataState.getInstance().getWorking_db(),
                "-ro",
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        long check = MessageCode.RO_DB_DOESNT_EXIST.getLong();

        assertTrue("Return Code Failure: " + rtn + " expecting: " + check, rtn == check);
    }

    @Test
    public void test_so_ro_sync_leg() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-db", DataState.getInstance().getWorking_db(),
                "-ro", "-sync", "-sql",
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        long check = MessageCode.RO_DB_DOESNT_EXIST.getLong();

        assertTrue("Return Code Failure: " + rtn + " expecting: " + check, rtn == check);
    }

    @Test
    public void test_so_ro_tf_leg() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-db", DataState.getInstance().getWorking_db(),
                "-tf", "call_center|store_sales", "-ro", "-sql",
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        long check = MessageCode.RO_DB_DOESNT_EXIST.getLong();

        assertTrue("Return Code Failure: " + rtn + " expecting: " + check, rtn == check);
    }

    @Test
    public void test_sql_ro_leg() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL", "-db", DataState.getInstance().getWorking_db(),
                "-sql", "-ro",
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        // Should fail because DB dir doesn't exist.  RO assumes data moved already.
        long check = MessageCode.RO_DB_DOESNT_EXIST.getLong();

        assertTrue("Return Code Failure: " + rtn + " expecting: " + check, rtn == check);
    }

    @Test
    public void test_sql_sync_leg() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-db", DataState.getInstance().getWorking_db(),
                "-d", "SQL", "-sync",
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        long check = MessageCode.VALID_SYNC_STRATEGIES.getLong();

    }

    @Test
    public void test_acid_common_all_leg() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-d", "COMMON", "-db", DataState.getInstance().getWorking_db(),
                "-sql", "-ma",
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);

        long check = MessageCode.VALID_ACID_STRATEGIES.getLong();

        assertTrue("Return Code Failure: " + rtn + " expecting: " + check, rtn == check);
    }

}