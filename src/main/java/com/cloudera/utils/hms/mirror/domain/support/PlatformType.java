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

package com.cloudera.utils.hms.mirror.domain.support;

import com.cloudera.utils.hms.mirror.domain.core.HiveServer2Config;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.cloudera.utils.hms.mirror.domain.support.HiveVersion.*;

@Getter
@RequiredArgsConstructor
public enum PlatformType {

    // No longer supported, until we have a need.
//    MAPR("MAPR", HIVE_1, Boolean.FALSE, Boolean.FALSE),
//    EMR4("EMR_4", HIVE_1, Boolean.FALSE, Boolean.FALSE),
//    EMR5("EMR_5", HIVE_2, Boolean.FALSE, Boolean.FALSE),
//    APACHE_HIVE1("APACHE_HIVE1", HIVE_1, Boolean.FALSE, Boolean.FALSE),
    APACHE_HIVE2("APACHE_HIVE2", HIVE_2, Boolean.FALSE, Boolean.FALSE),
    CDH5("CDH5", HIVE_2, Boolean.FALSE, Boolean.FALSE),
    CDH6("CDH6", HIVE_2, Boolean.FALSE, Boolean.FALSE),
    CDP7_0("CDP", HIVE_3, Boolean.FALSE, Boolean.FALSE),
    CDP7_1("CDP", HIVE_3, Boolean.FALSE, Boolean.FALSE),
    CDP7_1_9_SP1("CDP", HIVE_3, Boolean.TRUE, Boolean.FALSE),
    CDP7_2("CDP", HIVE_3, Boolean.TRUE, Boolean.TRUE),
    CDP7_3("CDP", HIVE_4, Boolean.TRUE, Boolean.TRUE),
    HDP2("HDP2", HIVE_2, Boolean.FALSE, Boolean.FALSE),
    HDP3("HDP3", HIVE_3, Boolean.FALSE, Boolean.FALSE),
    EMR6("EMR_6", HIVE_3, Boolean.FALSE, Boolean.FALSE),
    EMR7("EMR_7", HIVE_3, Boolean.FALSE, Boolean.FALSE),
    APACHE_HIVE3("APACHE_HIVE3", HIVE_3, Boolean.FALSE, Boolean.FALSE),
    APACHE_HIVE4("APACHE_HIVE4", HIVE_4, Boolean.TRUE, Boolean.TRUE);

    @NonNull
    private final String platform;
    @NonNull
    private final HiveVersion hiveVersion;
    @NonNull
    private final boolean dbOwnerType;
    @NonNull
    private final boolean icebergSupport;

    public boolean isLegacyHive() {
        return hiveVersion.isLegacy();
    }

    public boolean isHdpHive3() {
        if (this == PlatformType.HDP3) {
            return Boolean.TRUE;
        } else {
            return Boolean.FALSE;
        }
    }

}
