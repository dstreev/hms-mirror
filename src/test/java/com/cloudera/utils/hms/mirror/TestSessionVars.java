/*
 * Copyright (c) 2025. Cloudera, Inc. All Rights Reserved
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


package com.cloudera.utils.hms.mirror;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import static com.cloudera.utils.hms.mirror.SessionVars.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
//import static org.junit.jupiter.api.Assertions.assertEquals;

import java.text.MessageFormat;

@Slf4j
public class TestSessionVars {

    @Test
    public void testSetSessionVars() {
        String result = MessageFormat.
                format(SessionVars.SET_SESSION_VALUE_INT, SessionVars.SORT_DYNAMIC_PARTITION_THRESHOLD, -1);
        assertEquals("SET hive.optimize.sort.dynamic.partition.threshold=-1", result);
    }

    @Test
    public void testSetSessionVars_01() {
        String result = MessageFormat.format(SET_SESSION_VALUE_INT,HIVE_MAX_DYNAMIC_PARTITIONS ,
                        (int)(1322 * 1.2));
        assertEquals("SET hive.exec.max.dynamic.partitions=1586", result);
    }

    @Test
    public void testSetSessionVars_02() {
        String result = MessageFormat.format(SET_SESSION_VALUE_STRING,SORT_DYNAMIC_PARTITION ,
                "true");
        assertEquals("SET hive.optimize.sort.dynamic.partition=true", result);
    }

}
