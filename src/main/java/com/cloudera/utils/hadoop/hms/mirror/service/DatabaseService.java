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

package com.cloudera.utils.hadoop.hms.mirror.service;

import com.cloudera.utils.hadoop.hms.mirror.*;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.text.MessageFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

@Service
@Slf4j
public class DatabaseService {


    @Getter
    private ConnectionPoolService connectionPoolService;
    @Getter
    private ConfigService configService;

    @Autowired
    public void setConnectionPoolService(ConnectionPoolService connectionPoolService) {
        this.connectionPoolService = connectionPoolService;
    }

    @Autowired
    public void setConfigService(ConfigService configService) {
        this.configService = configService;
    }

    public Boolean getDatabase(Config config, DBMirror dbMirror) throws SQLException {
        Boolean rtn = Boolean.FALSE;
        Connection conn = null;
        try {
            conn = getConnection();
            if (conn != null) {

                String database = (getEnvironment() == Environment.LEFT ? dbMirror.getName() : config.getResolvedDB(dbMirror.getName()));

                log.debug(getEnvironment() + ":" + database + ": Loading database definition.");

                Statement stmt = null;
                ResultSet resultSet = null;
                try {
                    stmt = conn.createStatement();
                    log.debug(getEnvironment() + ":" + database + ": Getting Database Definition");
                    resultSet = stmt.executeQuery(MessageFormat.format(MirrorConf.DESCRIBE_DB, database));
                    //Retrieving the ResultSetMetaData object
                    ResultSetMetaData rsmd = resultSet.getMetaData();
                    //getting the column type
                    int column_count = rsmd.getColumnCount();
                    Map<String, String> dbDef = new TreeMap<String, String>();
                    while (resultSet.next()) {
                        for (int i = 0; i < column_count; i++) {
                            String cName = rsmd.getColumnName(i + 1).toUpperCase(Locale.ROOT);
                            String cValue = resultSet.getString(i + 1);
                            // Don't add element if its empty.
                            if (cValue != null && !cValue.trim().isEmpty()) {
                                dbDef.put(cName, cValue);
                            }
                        }
                    }
                    dbMirror.setDBDefinition(getEnvironment(), dbDef);
                    rtn = Boolean.TRUE;
                } catch (SQLException sql) {
                    // DB Doesn't Exists.
                } finally {
                    if (resultSet != null) {
                        try {
                            resultSet.close();
                        } catch (SQLException sqlException) {
                            // ignore
                        }
                    }
                    if (stmt != null) {
                        try {
                            stmt.close();
                        } catch (SQLException sqlException) {
                            // ignore
                        }
                    }
                }
            }
        } catch (SQLException se) {
            se.printStackTrace();
            throw se;
        } finally {
            try {
                if (conn != null)
                    conn.close();
            } catch (SQLException throwables) {
                //
            }
        }
        return rtn;
    }

    public Boolean runDatabaseSql(DBMirror dbMirror) {
        List<Pair> dbPairs = dbMirror.getSql(environment);
        Boolean rtn = Boolean.TRUE;
        for (Pair pair : dbPairs) {
            if (!runDatabaseSql(dbMirror, pair)) {
                rtn = Boolean.FALSE;
                // don't continue
                break;
            }
        }
        return rtn;
    }

    public Boolean runDatabaseSql(DBMirror dbMirror, Pair dbSqlPair) {
        // Open the connection and ensure we are running this on the "RIGHT" cluster.
        Connection conn = null;
        Boolean rtn = Boolean.TRUE;
        // Skip when running test data.
        if (!getConfig().isLoadingTestData()) {
            try {
                conn = getConnection();

                if (conn == null && getConfig().isExecute() && !this.getHiveServer2().isDisconnected()) {
                    // this is a problem.
                    rtn = Boolean.FALSE;
                    dbMirror.addIssue(getEnvironment(), "Connection missing. This is a bug.");
                }

                if (conn == null && this.getHiveServer2().isDisconnected()) {
                    dbMirror.addIssue(getEnvironment(), "Running in 'disconnected' mode.  NO RIGHT operations will be done.  " +
                            "The scripts will need to be run 'manually'.");
                }

                if (conn != null) {
                    if (dbMirror != null)
                        log.debug(getEnvironment() + " - " + dbSqlPair.getDescription() + ": " + dbMirror.getName());
                    else
                        log.debug(getEnvironment() + " - " + dbSqlPair.getDescription() + ":" + dbSqlPair.getAction());

                    Statement stmt = null;
                    try {
                        try {
                            stmt = conn.createStatement();
                        } catch (SQLException throwables) {
                            log.error("Issue building statement", throwables);
                            rtn = Boolean.FALSE;
                        }

                        try {
                            log.debug(getEnvironment() + ":" + dbSqlPair.getDescription() + ":" + dbSqlPair.getAction());
                            if (getConfig().isExecute()) // on dry-run, without db, hard to get through the rest of the steps.
                                stmt.execute(dbSqlPair.getAction());
                        } catch (SQLException throwables) {
                            log.error(getEnvironment() + ":" + dbSqlPair.getDescription() + ":", throwables);
                            dbMirror.addIssue(getEnvironment(), throwables.getMessage() + " " + dbSqlPair.getDescription() +
                                    " " + dbSqlPair.getAction());
                            rtn = Boolean.FALSE;
                        }

                    } finally {
                        if (stmt != null) {
                            try {
                                stmt.close();
                            } catch (SQLException sqlException) {
                                // ignore
                            }
                        }
                    }
                }
            } catch (SQLException throwables) {
                log.error(getEnvironment().toString(), throwables);
                throw new RuntimeException(throwables);
            } finally {
                try {
                    if (conn != null)
                        conn.close();
                } catch (SQLException throwables) {
                    //
                }
            }
        }
        return rtn;
    }

}
