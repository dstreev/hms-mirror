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

package com.cloudera.utils.hms.mirror.service;

import com.cloudera.utils.hms.mirror.MirrorConf;
import com.cloudera.utils.hms.mirror.Pair;
import com.cloudera.utils.hms.mirror.domain.core.DBMirror;
import com.cloudera.utils.hms.mirror.domain.core.EnvironmentTable;
import com.cloudera.utils.hms.mirror.domain.core.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.core.TableMirror;
import com.cloudera.utils.hms.mirror.domain.support.*;
import com.cloudera.utils.hms.mirror.reporting.ReportingConf;
import com.cloudera.utils.hms.util.TableUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Service for handling ConversionResult business operations.
 * This service contains the business logic that was previously embedded in the ConversionResult domain object.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class ConversionResultService {

    private final ExecuteSessionService executeSessionService;

//    /**
//     * Generates action SQL script for a specific environment and database.
//     * NOTE: Currently commented out because it depends on TableMirror.getTableActions() and
//     * EnvironmentTable.actions field which are also commented out.
//     *
//     * @param conversionResult The conversion result containing database mirrors
//     * @param env             The target environment
//     * @param database        The database name
//     * @return SQL script as a string
//     */
//    public String actionsSql(ConversionResult conversionResult, Environment env, String database) {
//        StringBuilder sb = new StringBuilder();
//        sb.append("-- ACTION script for ").append(env).append(" cluster\n\n");
//        sb.append("-- HELPER Script to assist with MANUAL updates.\n");
//        sb.append("-- RUN AT OWN RISK  !!!\n");
//        sb.append("-- REVIEW and UNDERSTAND the adjustments below before running.\n\n");
//        DBMirror dbMirror = conversionResult.getDatabases().get(database);
//        sb.append("-- DATABASE: ").append(database).append("\n");
//        Set<String> tables = dbMirror.getTableMirrors().keySet();
//        for (String table : tables) {
//            TableMirror tblMirror = dbMirror.getTableMirrors().get(table);
//            sb.append("--    Table: ").append(table).append("\n");
//            // LEFT Table Actions
//            for (String item : tblMirror.getTableActions(env)) {
//                sb.append(item).append(";\n");
//            }
//            sb.append("\n");
//        }
//        return sb.toString();
//    }

    /**
     * Generates cleanup SQL script for a specific environment and database.
     *
     * @param conversionResult The conversion result containing database mirrors
     * @param environment     The target environment
     * @param database        The database name
     * @return SQL cleanup script as a string, or null if no cleanup is needed
     */
    public String executeCleanUpSql(ConversionResult conversionResult, Environment environment, String database) {
        StringBuilder sb = new StringBuilder();
        boolean found = Boolean.FALSE;
        sb.append("-- EXECUTION CLEANUP script for ").append(database).append(" on ").append(environment).append(" cluster\n\n");
        sb.append("-- ").append(new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new Date())).append("\n\n");

        DBMirror dbMirror = conversionResult.getDatabases().get(database);

        Set<String> tables = dbMirror.getTableMirrors().keySet();
        for (String table : tables) {
            TableMirror tblMirror = dbMirror.getTableMirrors().get(table);
            if (tblMirror.isThereCleanupSql(environment)) {
                sb.append("\n--    Cleanup script: ").append(table).append("\n");
                for (Pair pair : tblMirror.getCleanUpSql(environment)) {
                    sb.append(pair.getAction());
                    // Skip ';' when it's a comment
                    // https://github.com/cloudera-labs/hms-mirror/issues/33
                    if (!pair.getAction().trim().startsWith("--")) {
                        sb.append(";\n");
                        found = Boolean.TRUE;
                    } else {
                        sb.append("\n");
                    }
                }
            } else {
                sb.append("\n");
            }
        }
        if (found)
            return sb.toString();
        else
            return null;
    }

    /**
     * Generates execution SQL script for a specific environment and database.
     *
     * @param conversionResult The conversion result containing database mirrors
     * @param environment     The target environment
     * @param database        The database name
     * @return SQL execution script as a string, or null if no SQL is needed
     */
    public String executeSql(ConversionResult conversionResult, Environment environment, String database) {
        StringBuilder sb = new StringBuilder();
        Boolean found = Boolean.FALSE;
        sb.append("-- EXECUTION script for ").append(database).append(" on ").append(environment).append(" cluster\n\n");
        sb.append("-- ").append(new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new Date()));
        sb.append("-- These are the command run on the ").append(environment).append(" cluster when `-e` is used.\n");
        DBMirror dbMirror = conversionResult.getDatabases().get(database);

        List<Pair> dbSql = dbMirror.getSql(environment);
        if (dbSql != null && !dbSql.isEmpty()) {
            for (Pair sqlPair : dbSql) {
                sb.append("-- ").append(sqlPair.getDescription()).append("\n");
                sb.append(sqlPair.getAction()).append(";\n");
                found = Boolean.TRUE;
            }
        }

        Set<String> tables = dbMirror.getTableMirrors().keySet();
        for (String table : tables) {
            TableMirror tblMirror = dbMirror.getTableMirrors().get(table);
            sb.append("\n--    Table: ").append(table).append("\n");
            if (tblMirror.isThereSql(environment)) {
                for (Pair pair : tblMirror.getSql(environment)) {
                    sb.append(pair.getAction()).append(";\n");
                    found = Boolean.TRUE;
                }
            } else {
                sb.append("\n");
            }
        }
        if (found)
            return sb.toString();
        else
            return null;
    }

    /**
     * Generates a detailed report for a specific database.
     *
     * @param conversionResult      The conversion result containing database information
     * @param database             The database name
     * @param executeSessionService The session service providing configuration and status
     * @return Markdown report as a string
     * @throws JsonProcessingException if there's an error processing JSON/YAML
     */
    public String toReport(ConversionResult conversionResult, String database, ExecuteSessionService executeSessionService) throws JsonProcessingException {
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();
        RunStatus runStatus = executeSessionService.getSession().getRunStatus();

        StringBuilder sb = new StringBuilder();
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        sb.append("# HMS-Mirror for: ").append(database).append("\n\n");
        sb.append(ReportingConf.substituteVariablesFromManifest("v.${HMS-Mirror-Version}")).append("\n");
        sb.append("---\n").append("## Run Log\n\n");
        sb.append("| Date | Elapsed Time | Status\n");
        sb.append("|:---|:---|:---|\n");
        Date current = new Date();
        BigDecimal elsecs = new BigDecimal(runStatus.getDuration())
                .divide(new BigDecimal(1000), 2, RoundingMode.HALF_UP);
        DecimalFormat eldecf = new DecimalFormat("#,###.00");
        DecimalFormat lngdecf = new DecimalFormat("#,###");
        String elsecStr = eldecf.format(elsecs);

        sb.append("| ").append(df.format(new Date()))
                .append(" | ").append(elsecStr).append(" secs | ")
                .append(runStatus.getProgress()).append("|\n\n");

        sb.append("**Comment**\n");
        sb.append("> ").append(runStatus.getComment()).append("\n\n");

        sb.append("## Config:\n");

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.enable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        String yamlStr = mapper.writeValueAsString(hmsMirrorConfig);
        // Mask User/Passwords in Control File
        yamlStr = yamlStr.replaceAll("user:\\s\".*\"", "user: \"*****\"");
        yamlStr = yamlStr.replaceAll("password:\\s\".*\"", "password: \"*****\"");
        sb.append("```\n");
        sb.append(yamlStr).append("\n");
        sb.append("```\n\n");

        if (runStatus.hasConfigMessages()) {
            sb.append("### Config Adjustment Messages:\n");
            for (String message : runStatus.getConfigMessages()) {
                sb.append("- ").append(message).append("\n");
            }
            sb.append("\n");
        }
        if (runStatus.hasErrors()) {
            sb.append("### Config Errors:\n");
            for (String message : runStatus.getErrorMessages()) {
                sb.append("- ").append(message).append("\n");
            }
            sb.append("\n");
        }
        if (runStatus.hasWarnings()) {
            sb.append("### Config Warnings:\n");
            for (String message : runStatus.getWarningMessages()) {
                sb.append("- ").append(message).append("\n");
            }
            sb.append("\n");
        }

        DBMirror dbMirror = conversionResult.getDatabases().get(database);

        sb.append("## Database SQL Statement(s)").append("\n\n");

        for (Environment environment : Environment.values()) {
            if (dbMirror.getSql(environment) != null && !dbMirror.getSql(environment).isEmpty()) {
                sb.append("### ").append(environment.toString()).append("\n\n");
                sb.append("```\n");
                for (Pair sqlPair : dbMirror.getSql(environment)) {
                    sb.append("-- ").append(sqlPair.getDescription()).append("\n");
                    sb.append(sqlPair.getAction()).append("\n");
                }
                sb.append("```\n");
            }
        }

        sb.append("\n");

        sb.append("## DB Issues").append("\n\n");
        // Issues
        if (dbMirror.isThereAnIssue()) {
            Set<Environment> environments = new HashSet<>();
            environments.add(Environment.LEFT);
            environments.add(Environment.RIGHT);

            for (Environment env : environments) {
                if (dbMirror.getProblemSQL().get(env) != null) {
                    sb.append("### Problem SQL Statements for ").append(env.toString()).append("\n\n");
                    sb.append("```\n");
                    // For each entry in the dbMirror.getProblemSQL().get(environment) print the SQL
                    for (Map.Entry<String, String> sqlItem : dbMirror.getProblemSQL().get(env).entrySet()) {
                        sb.append("-- ").append(sqlItem.getValue()).append("\n");
                        sb.append(sqlItem.getKey()).append("\n");
                    }
                    sb.append("```\n");
                }

                List<String> issues = dbMirror.getIssuesList(env);
                if (issues != null) {
                    sb.append("### Advisories for ").append(env).append("\n\n");
                    for (String issue : issues) {
                        sb.append("* ").append(issue).append("\n");
                    }
                }
            }

        } else {
            sb.append("none\n");
        }

        sb.append("\n## Table Status (").append(dbMirror.getTableMirrors().size()).append(")  ");
        sb.append(dbMirror.getPhaseSummaryString()).append("\n\n");

        sb.append("*NOTE* SQL in this report may be altered by the renderer.  Do NOT COPY/PASTE from this report.  Use the LEFT|RIGHT_execution.sql files for accurate scripts\n\n");

        sb.append("<table>").append("\n");
        sb.append("<tr>").append("\n");
        sb.append("<th style=\"test-align:left\">Table</th>").append("\n");
        sb.append("<th style=\"test-align:left\">Strategy</th>").append("\n");
        sb.append("<th style=\"test-align:left\">Source<br/>Managed</th>").append("\n");
        sb.append("<th style=\"test-align:left\">Source<br/>ACID</th>").append("\n");
        sb.append("<th style=\"test-align:left\">Phase<br/>State</th>").append("\n");
        sb.append("<th style=\"test-align:right\">Duration</th>").append("\n");
//        sb.append("<th style=\"test-align:right\">Partition<br/>Count</th>").append("\n");
        sb.append("<th style=\"test-align:left\">Steps</th>").append("\n");
//        if (dbMirror.hasActions()) {
//            sb.append("<th style=\"test-align:left\">Actions</th>").append("\n");
//        }
        if (dbMirror.hasAddedProperties()) {
            sb.append("<th style=\"test-align:left\">Added<br/>Properties</th>").append("\n");
        }
        if (dbMirror.hasStatistics()) {
            sb.append("<th style=\"test-align:left\">Stats</th>").append("\n");
        }
        if (dbMirror.hasIssues() || dbMirror.hasErrors()) {
            sb.append("<th style=\"test-align:left\">Issues</th>").append("\n");
        }
        sb.append("<th style=\"test-align:left\">SQL</th>").append("\n");
        sb.append("</tr>").append("\n");

        Set<String> tables = dbMirror.getTableMirrors().keySet();
        for (String table : tables) {
            sb.append("<tr>").append("\n");
            TableMirror tblMirror = dbMirror.getTableMirrors().get(table);
            EnvironmentTable let = tblMirror.getEnvironmentTable(Environment.LEFT);
            // table
            sb.append("<td>").append(table).append("</td>").append("\n");
            // Strategy
            sb.append("<td>").append(tblMirror.getStrategy()).append("</td>").append("\n");
            // Source Managed
            sb.append("<td>");
            if (TableUtils.isManaged(let)) {
                sb.append("X");
            }
            sb.append("</td>").append("\n");
            // Source ACID
            sb.append("<td>").append("\n");
            if (TableUtils.isACID(let)) {
                sb.append("X");
            }
            sb.append("</td>").append("\n");
            // phase state
            sb.append("<td>").append(tblMirror.getPhaseState().toString()).append("</td>").append("\n");

            // Stage Duration
            BigDecimal secs = new BigDecimal(tblMirror.getStageDuration()).divide(new BigDecimal(1000));///1000
            DecimalFormat decf = new DecimalFormat("#,###.00");
            String secStr = decf.format(secs);
            sb.append("<td>").append(secStr).append("</td>").append("\n");

            // Partition Count
//            sb.append("<td>").append(let.getPartitioned() ?
//                    let.getPartitions().size() : " ").append("</td>").append("\n");

            // Steps
            sb.append("<td>\n");
            sb.append("<table>\n");
            for (com.cloudera.utils.hms.mirror.Marker entry : tblMirror.getSteps()) {
                sb.append("<tr>\n");
                sb.append("<td>");
                sb.append(entry.getMark());
                sb.append("</td>");
                sb.append("<td>");
                sb.append(entry.getDescription());
                sb.append("</td>");
                sb.append("<td>");
                if (entry.getAction() != null)
                    sb.append(entry.getAction());
                sb.append("</td>");
                sb.append("</tr>\n");
            }
            sb.append("</table>\n");
            sb.append("</td>\n");

            // Actions
//            if (dbMirror.hasActions()) {
//                // LEFT Table Actions
//                Iterator<String> a1Iter = tblMirror.getTableActions(Environment.LEFT).iterator();
//                sb.append("<td>").append("\n");
//                sb.append("<table>");
//                while (a1Iter.hasNext()) {
//                    sb.append("<tr>");
//                    String item = a1Iter.next();
//                    sb.append("<td style=\"text-align:left\">").append(item).append(";</td>");
//                    sb.append("</tr>");
//                }
//                sb.append("</table>");
//                sb.append("</td>").append("\n");
//
//                // RIGHT Table Actions
//                Iterator<String> a2Iter = tblMirror.getTableActions(Environment.RIGHT).iterator();
//                sb.append("<td>").append("\n");
//                sb.append("<table>");
//                while (a2Iter.hasNext()) {
//                    sb.append("<tr>");
//                    String item = a2Iter.next();
//                    sb.append("<td style=\"text-align:left\">").append(item).append(";</td>");
//                    sb.append("</tr>");
//                }
//                sb.append("</table>");
//                sb.append("</td>").append("\n");
//            }

            // Properties
            if (dbMirror.hasAddedProperties()) {
                sb.append("<td>").append("\n");
                sb.append("<table>");
                for (Map.Entry<Environment, EnvironmentTable> entry : tblMirror.getEnvironments().entrySet()) {
                    if (!entry.getValue().getAddProperties().isEmpty()) {
                        sb.append("<tr>\n");
                        sb.append("<th colspan=\"2\">");
                        sb.append(entry.getKey());
                        sb.append("</th>\n");
                        sb.append("</tr>").append("\n");

                        for (Map.Entry<String, String> prop : entry.getValue().getAddProperties().entrySet()) {
                            sb.append("<tr>\n");
                            sb.append("<td>");
                            sb.append(prop.getKey());
                            sb.append("</td>\n");
                            sb.append("<td>");
                            sb.append(prop.getValue());
                            sb.append("</td>\n");
                            sb.append("</tr>\n");
                        }
                    }
                }
                sb.append("</table>");
                sb.append("</td>").append("\n");
            }
            // Statistics
            if (dbMirror.hasStatistics()) {
                sb.append("<td>").append("\n");
                sb.append("<table>");
                for (Map.Entry<Environment, EnvironmentTable> entry : tblMirror.getEnvironments().entrySet()) {
                    if (!entry.getValue().getStatistics().isEmpty()) {
                        sb.append("<tr>\n");
                        sb.append("<th colspan=\"2\">");
                        sb.append(entry.getKey());
                        sb.append("</th>\n");
                        sb.append("</tr>").append("\n");

                        for (Map.Entry<String, Object> prop : entry.getValue().getStatistics().entrySet()) {
                            sb.append("<tr>\n");
                            sb.append("<td>");
                            sb.append(prop.getKey());
                            sb.append("</td>\n");
                            sb.append("<td>");
                            if (prop.getValue() instanceof Double || prop.getValue() instanceof Long) {
                                sb.append(lngdecf.format(prop.getValue()));
                            } else {
                                sb.append(prop.getValue().toString());
                            }
                            sb.append("</td>\n");
                            sb.append("</tr>\n");
                        }
                        if (entry.getValue().getPartitioned()) {
                            sb.append("<tr>\n");
                            sb.append("<td>");
                            sb.append(MirrorConf.PARTITION_COUNT);
                            sb.append("</td>\n");
                            sb.append("<td>");
                            sb.append(entry.getValue().getPartitions().size());
                            sb.append("</td>\n");
                            sb.append("</tr>\n");
                        }
                    }
                }
                sb.append("</table>");
                sb.append("</td>").append("\n");
            }
            // Issues Reporting
            if (dbMirror.hasIssues() || dbMirror.hasErrors()) {
                sb.append("<td>").append("\n");
                sb.append("<table>");
                for (Map.Entry<Environment, EnvironmentTable> entry : tblMirror.getEnvironments().entrySet()) {
                    if (!entry.getValue().getIssues().isEmpty()) {
                        sb.append("<tr>\n");
                        sb.append("<th>");
                        sb.append(entry.getKey());
                        sb.append("</th>\n");
                        sb.append("</tr>").append("\n");

                        for (String issue : entry.getValue().getIssues()) {
                            sb.append("<tr>\n");
                            sb.append("<td>");
                            sb.append(issue);
                            sb.append("</td>\n");
                            sb.append("</tr>\n");
                        }
                    }
                    if (!entry.getValue().getErrors().isEmpty()) {
                        sb.append("<tr>\n");
                        sb.append("<th>");
                        sb.append(entry.getKey()).append(" - Error(s)");
                        sb.append("</th>\n");
                        sb.append("</tr>").append("\n");
                        sb.append("<tr>\n");
                        sb.append("<td>");
                        sb.append("<ul>");
                        for (String error : entry.getValue().getErrors()) {
                            sb.append("<li>");
                            sb.append(error);
                            sb.append("</li>");
                        }
                        sb.append("</ul>");
                        sb.append("</td>\n");
                        sb.append("</tr>\n");
                    }
                }
                sb.append("</table>");
                sb.append("</td>").append("\n");
            }
            // SQL Output
            sb.append("<td>\n");
            sb.append("<table>");
            for (Map.Entry<Environment, EnvironmentTable> entry : tblMirror.getEnvironments().entrySet()) {
                if (!entry.getValue().getSql().isEmpty()) {
                    sb.append("<tr>\n");
                    sb.append("<th colspan=\"2\">");
                    sb.append(entry.getKey());
                    sb.append("</th>\n");
                    sb.append("</tr>").append("\n");

                    for (Pair pair : entry.getValue().getSql()) {
                        sb.append("<tr>\n");
                        sb.append("<td>");
                        sb.append(pair.getDescription());
                        sb.append("</td>\n");
                        sb.append("<td>");
                        sb.append(pair.getAction());
                        sb.append("</td>\n");
                        sb.append("</tr>\n");
                    }
                    if (!entry.getValue().getCleanUpSql().isEmpty()) {
                        sb.append("<tr>\n");
                        sb.append("<th colspan=\"2\">");
                        sb.append("=== SQL CleanUp ===");
                        sb.append("</th>\n");
                        sb.append("</tr>").append("\n");
                    }
                    for (Pair pair : entry.getValue().getCleanUpSql()) {
                        sb.append("<tr>\n");
                        sb.append("<td>");
                        sb.append(pair.getDescription());
                        sb.append("</td>\n");
                        sb.append("<td>");
                        sb.append(pair.getAction());
                        sb.append("</td>\n");
                        sb.append("</tr>\n");
                    }
                }
            }
            sb.append("</table>");
            sb.append("</td>").append("\n");
            sb.append("</tr>").append("\n");
        }
        sb.append("</table>").append("\n");

        if (!dbMirror.getFilteredOut().isEmpty()) {
            sb.append("\n## Skipped Tables/Views\n\n");

            sb.append("| Table / View | Reason |\n");
            sb.append("|:---|:---|\n");
            for (Map.Entry<String, String> entry : dbMirror.getFilteredOut().entrySet()) {
                sb.append("| ").append(entry.getKey()).append(" | ").append(entry.getValue()).append(" |\n");
            }
        }
        return sb.toString();
    }

    /**
     * Counts the number of tables that have not been successfully processed.
     * Tables are considered unsuccessful if their phase state is not CALCULATED_SQL or PROCESSED.
     *
     * @param conversionResult The conversion result containing database mirrors
     * @return The count of unsuccessful tables
     */
    public int getUnsuccessfullTableCount(ConversionResult conversionResult) {
        int count = 0;
        for (DBMirror dbMirror : conversionResult.getDatabases().values()) {
            for (TableMirror tableMirror : dbMirror.getTableMirrors().values()) {
                switch (tableMirror.getPhaseState()) {
                    // Don't count successful conversions.
                    case CALCULATED_SQL:
                    case PROCESSED:
                        break;
                    default:
                        count++;
                }
            }
        }
        return count;
    }
}
