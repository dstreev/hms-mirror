/*
 * Copyright (c) 2023. Cloudera, Inc. All Rights Reserved
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

package com.cloudera.utils.hadoop.hms.stage;

import com.cloudera.utils.hadoop.hms.Context;
import com.cloudera.utils.hadoop.hms.mirror.Config;
import com.cloudera.utils.hadoop.hms.mirror.Conversion;
import com.cloudera.utils.hadoop.hms.mirror.DBMirror;
import com.cloudera.utils.hadoop.hms.mirror.Environment;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Callable;

@Slf4j
public class CreateDatabases implements Callable<ReturnStatus> {
//    private static final Logger log = LoggerFactory.getLogger(CreateDatabases.class);

    private Conversion conversion = null;

    // Flag use to determine is creating transition db in lower cluster
    // or target db in upper cluster.
    private final boolean successful = Boolean.FALSE;

    public boolean isSuccessful() {
        return successful;
    }

    public CreateDatabases(Conversion conversion) {
        this.conversion = conversion;
    }

    @Override
    public ReturnStatus call() {
        ReturnStatus rtn = new ReturnStatus();
        Config config = Context.getInstance().getConfig();
        log.debug("Create Databases");
        for (String database : config.getDatabases()) {
            DBMirror dbMirror = conversion.getDatabase(database);

            dbMirror.buildDBStatements();

            if (config.getCluster(Environment.LEFT).runDatabaseSql(dbMirror)) {
                rtn.setStatus(ReturnStatus.Status.SUCCESS);
            } else {
                rtn.setStatus(ReturnStatus.Status.ERROR);
            }

            if (config.getCluster(Environment.RIGHT).runDatabaseSql(dbMirror)) {
                rtn.setStatus(ReturnStatus.Status.SUCCESS);
            } else {
                rtn.setStatus(ReturnStatus.Status.ERROR);
            }
        }
        return rtn;
    }
}
