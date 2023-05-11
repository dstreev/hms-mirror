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

package com.cloudera.utils.hadoop.hms.stage;

import com.cloudera.utils.hadoop.hms.mirror.*;
import com.cloudera.utils.hadoop.hms.mirror.*;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.math.RoundingMode;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.*;

import static com.cloudera.utils.hadoop.hms.mirror.MessageCode.*;

/*
Using the config, go through the databases and tables and collect the current states.

Create the target databases, where needed to support the migration.
 */
public class Setup {
    private static final Logger LOG = LogManager.getLogger(Setup.class);

    private Config config = null;
    private Conversion conversion = null;

    public Setup(Config config, Conversion conversion) {
        this.config = config;
        this.conversion = conversion;
    }

    // TODO: Need to address failures here...
    public Boolean collect() {
        Boolean rtn = Boolean.TRUE;
        Date startTime = new Date();
        LOG.info("GATHERING METADATA: Start Processing for databases: " + Arrays.toString((config.getDatabases())));

        List<ScheduledFuture<ReturnStatus>> gtf = new ArrayList<ScheduledFuture<ReturnStatus>>();
        for (String database : config.getDatabases()) {
            DBMirror dbMirror = conversion.addDatabase(database);
            try {
                // Get the Database definitions for the LEFT and RIGHT clusters.
                if (config.getCluster(Environment.LEFT).getDatabase(config, dbMirror)) {
                    config.getCluster(Environment.RIGHT).getDatabase(config, dbMirror);
                } else {
                    // LEFT DB doesn't exists.
                    dbMirror.addIssue(Environment.LEFT, "DB doesn't exist. Check permissions for user running process");
                    return Boolean.FALSE;
                }
            } catch (SQLException se) {
                throw new RuntimeException(se);
            }

            // Build out the table in a database.
            if (!config.getDatabaseOnly()) {
                Callable<ReturnStatus> gt = new GetTables(config, dbMirror);
                gtf.add(config.getTransferThreadPool().schedule(gt, 1, TimeUnit.MILLISECONDS));
            }
        }

        // Collect Table Information
        while (true) {
            boolean check = true;
            for (Future<ReturnStatus> sf : gtf) {
                if (!sf.isDone()) {
                    check = false;
                    break;
                }
                try {
                    if (sf.isDone() && sf.get() != null) {
                        if (sf.get().getStatus() == ReturnStatus.Status.ERROR) {
                            rtn = Boolean.FALSE;
//                            throw new RuntimeException(sf.get().getException());
                        }
                    }
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
            if (check)
                break;
        }
        gtf.clear(); // reset

        // Failure, report and exit with FALSE
        if (!rtn) {
            config.getErrors().set(COLLECTING_TABLES.getCode());
            return Boolean.FALSE;
        }

        // Create the databases we'll need on the LEFT and RIGHT
        Callable<ReturnStatus> createDatabases = new CreateDatabases(config, conversion);
        gtf.add(config.getTransferThreadPool().schedule(createDatabases, 1, TimeUnit.MILLISECONDS));

        // Check and Build DB's First.
        while (true) {
            boolean check = true;
            for (Future<ReturnStatus> sf : gtf) {
                if (!sf.isDone()) {
                    check = false;
                    break;
                }
                try {
                    if (sf.isDone() && sf.get() != null) {
                        ReturnStatus returnStatus = sf.get();
                        if (returnStatus != null && returnStatus.getStatus() == ReturnStatus.Status.ERROR) {
//                            throw new RuntimeException(sf.get().getException());
                            rtn = Boolean.FALSE;
                        }
                    }
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
            if (check)
                break;
        }
        gtf.clear(); // reset

        // Failure, report and exit with FALSE
        if (!rtn) {
            config.getErrors().set(DATABASE_CREATION.getCode());
            return Boolean.FALSE;
        }

        // Shortcut.  Only DB's.
        if (!config.getDatabaseOnly()) {

            // Get the table METADATA for the tables collected in the databases.
            LOG.info(">>>>>>>>>>> Getting Table Metadata");
            Set<String> collectedDbs = conversion.getDatabases().keySet();
            for (String database : collectedDbs) {
                DBMirror dbMirror = conversion.getDatabase(database);
                Set<String> tables = dbMirror.getTableMirrors().keySet();
                for (String table : tables) {
                    TableMirror tblMirror = dbMirror.getTableMirrors().get(table);
                    GetTableMetadata tmd = new GetTableMetadata(config, dbMirror, tblMirror);
                    gtf.add(config.getMetadataThreadPool().schedule(tmd, 1, TimeUnit.MILLISECONDS));
                }
            }

            while (true) {
                boolean check = true;
                for (Future<ReturnStatus> sf : gtf) {
                    if (!sf.isDone()) {
                        check = false;
                        break;
                    }
                    try {
                        if (sf.isDone() && sf.get() != null) {
                            if (sf.get().getStatus() == ReturnStatus.Status.ERROR) {
                                throw new RuntimeException(sf.get().getException());
                            }
                        }
                    } catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                }
                if (check)
                    break;
            }
            gtf.clear(); // reset

            if (!rtn) {
                config.getErrors().set(COLLECTING_TABLE_DEFINITIONS.getCode());
            }

            LOG.info("==============================");
            LOG.info(conversion.toString());
            LOG.info("==============================");
            Date endTime = new Date();
            DecimalFormat df = new DecimalFormat("#.###");
            df.setRoundingMode(RoundingMode.CEILING);
            LOG.info("GATHERING METADATA: Completed in " + df.format((Double) ((endTime.getTime() - startTime.getTime()) / (double) 1000)) + " secs");
        }
        return rtn;
    }

}
