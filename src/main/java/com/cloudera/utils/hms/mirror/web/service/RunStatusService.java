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

package com.cloudera.utils.hms.mirror.web.service;

import com.cloudera.utils.hms.mirror.DBMirror;
import com.cloudera.utils.hms.mirror.PhaseState;
import com.cloudera.utils.hms.mirror.domain.support.Conversion;
import com.cloudera.utils.hms.mirror.domain.support.RunStatus;
import com.cloudera.utils.hms.mirror.service.ExecuteSessionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Objects;

import static org.apache.commons.lang3.StringUtils.isBlank;

@Service
public class RunStatusService {

    private ExecuteSessionService executeSessionService;

    @Autowired
    public void setExecuteSessionService(ExecuteSessionService executeSessionService) {
        this.executeSessionService = executeSessionService;
    }

    public RunStatus getRunStatus(String sessionId) {
        RunStatus runStatus = null;
        if (isBlank(sessionId)) {
            runStatus = executeSessionService.getSession().getRunStatus();
        } else {
            runStatus = executeSessionService.getSession(sessionId).getRunStatus();
        }

        Conversion conversion = executeSessionService.getSession().getConversion();

        // Reset the inProgressTables with what's currently working.
        runStatus.getInProgressTables().clear();

        for (String database : conversion.getDatabases().keySet()) {
            DBMirror dbMirror = conversion.getDatabase(database);
            for (String tbl : dbMirror.getTableMirrors().keySet()) {
                if (Objects.requireNonNull(dbMirror.getTable(tbl).getPhaseState()) == PhaseState.STARTED) {
                    runStatus.getInProgressTables().add(dbMirror.getTable(tbl));
                }
            }
        }

        return runStatus;
    }
}
