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

package com.cloudera.utils.hms.mirror.service;

import com.cloudera.utils.hms.mirror.domain.support.RunStatus;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Getter
@Slf4j
public class HMSMirrorAppService {

    private final HmsMirrorCfgService hmsMirrorCfgService;
    private final RunStatus runStatus;

    public HMSMirrorAppService(HmsMirrorCfgService hmsMirrorCfgService, RunStatus runStatus) {
        this.hmsMirrorCfgService = hmsMirrorCfgService;
        this.runStatus = runStatus;
    }

    public long getReturnCode() {
        long rtn = 0L;
        rtn = getRunStatus().getErrors().getReturnCode();
        // If app ran, then check for unsuccessful table conversions.
        if (rtn == 0) {
            rtn = getRunStatus().getConversion().getUnsuccessfullTableCount();
        }
        return rtn;
    }

}
