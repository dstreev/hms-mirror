/*
 * Copyright (c) 2022-2023. Cloudera, Inc. All Rights Reserved
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

package com.cloudera.utils.hadoop.hms;

import com.cloudera.utils.hadoop.hms.mirror.*;
import com.cloudera.utils.hadoop.hms.mirror.datastrategy.DataStrategyEnum;
import com.cloudera.utils.hadoop.hms.mirror.service.ConnectionPoolService;
import com.cloudera.utils.hadoop.hms.stage.ReturnStatus;
import com.cloudera.utils.hadoop.hms.mirror.service.TransferService;
import com.cloudera.utils.hadoop.hms.util.Protect;
import com.cloudera.utils.hive.config.DBStore;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.springframework.stereotype.Component;

import java.io.*;
import java.math.RoundingMode;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class MirrorLegacy {
//    private static final Logger log = LoggerFactory.getLogger(Mirror.class);
//    private final String leftActionFile = null;
//    private final String rightActionFile = null;
    private final Boolean retry = Boolean.FALSE;
//    private Conversion conversion = null;
//    private Config config = null;
//    private String configFile = null;
//    private String reportOutputDir = null;
//    private String reportOutputFile = null;
//    private String leftExecuteFile = null;
//    private String leftCleanUpFile = null;
//    private String rightExecuteFile = null;
//    private String rightCleanUpFile = null;
//    private Boolean quiet = Boolean.FALSE;
    private String dateMarker;

//    public static void main(String[] args) {
//        MirrorLegacy mirror = new MirrorLegacy();
//        System.exit((int) mirror.go(args));
//    }

//    public int doit() {
//        int rtn = 0;
////        if (getConfig().isLoadingTestData() && !getConfig().getReplay()) {
////            // Load conversion test data from a file.
////            try {
////                System.out.println("Test data file: " + getConfig().getLoadTestDataFile());
////                log.info("Check 'classpath' for test data file");
////                URL configURL = this.getClass().getResource(getConfig().getLoadTestDataFile());
////                if (configURL == null) {
////                    log.info("Checking filesystem for test data file");
////                    File conversionFile = new File(getConfig().getLoadTestDataFile());
////                    if (!conversionFile.exists())
////                        throw new RuntimeException("Couldn't locate test data file: " + getConfig().getLoadTestDataFile());
////                    configURL = conversionFile.toURI().toURL();
////                }
////                ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
////                mapper.enable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
////
////                String yamlCfgFile = IOUtils.toString(configURL, StandardCharsets.UTF_8);
////                conversion = mapper.readerFor(Conversion.class).readValue(yamlCfgFile);
////                // Set Config Databases;
////                String[] databases = conversion.getDatabases().keySet().toArray(new String[0]);
////                getConfig().setDatabases(databases);
////            } catch (UnrecognizedPropertyException upe) {
////                throw new RuntimeException("\nThere may have been a breaking change in the configuration since the previous " +
////                        "release. Review the note below and remove the 'Unrecognized field' from the configuration and try " +
////                        "again.\n\n", upe);
////            } catch (Throwable t) {
////                // Look for yaml update errors.
////                if (t.toString().contains("MismatchedInputException")) {
////                    throw new RuntimeException("The format of the 'config' yaml file MAY HAVE CHANGED from the last release.  Please make a copy and run " +
////                            "'-su|--setup' again to recreate in the new format", t);
////                } else {
////                    log.error(t.getMessage(), t);
////                    throw new RuntimeException("A configuration element is no longer valid, progress.  Please remove the element from the configuration yaml and try again.", t);
////                }
////            }
//////            }
////        } else if (!getConfig().getReplay()) {
////            // Replay would have loaded the conversion from the file already. Don't load it again.
////            conversion = new Conversion();
////        }
//
//        // Setup and Start the State Maintenance Routine
////        StateMaintenance stateMaintenance = new StateMaintenance(10000, configFile, getDateMarker());
//
//        // Link the conversion to the state machine.
////        stateMaintenance.setConversion(conversion);
//
//        /*
//        // Setup and Start the Reporter
//        Reporter reporter = new Reporter(conversion, 1000);
//        reporter.setQuiet(getQuiet());
//        reporter.setVariable("config.file", configFile);
//        reporter.setVariable("config.strategy", getConfig().getDataStrategy().toString());
//        reporter.setVariable("report.file", reportOutputFile);
//        reporter.setVariable("left.execute.file", leftExecuteFile);
//        reporter.setVariable("left.cleanup.file", leftCleanUpFile);
//        reporter.setVariable("right.execute.file", rightExecuteFile);
//        reporter.setVariable("right.cleanup.file", rightCleanUpFile);
////        reporter.setVariable("left.action.file", leftActionFile);
////        reporter.setVariable("right.action.file", rightActionFile);
//        reporter.setRetry(this.retry);
//        reporter.start();
//*/
//        Date startTime = new Date();
//        DateFormat df = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss");
//
//        if (getConfig().isExecute()) {
////            reporter.setVariable("run.mode", "EXECUTE");
//        } else {
////            reporter.setVariable("run.mode", "DRY-RUN");
//        }
////        Boolean setupError = Boolean.FALSE;
//
////        ObjectMapper mapper;
////        mapper = new ObjectMapper(new YAMLFactory());
////        mapper.enable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
////
////        // Skip Setup if working from 'retry'
//////        if (!getConfig().isLoadingTestData()) {
////        // This will collect all existing DB/Table Definitions in the clusters
////        Setup setup = new Setup(conversion);
////        // TODO: Failure here may not make it to saved state.
////        if (setup.collect()) {
//////                stateMaintenance.saveState();
////            // State reason table/view was removed from processing list.
////            for (Map.Entry<String, DBMirror> dbEntry : conversion.getDatabases().entrySet()) {
////                if (getConfig().getDatabaseOnly()) {
////                    dbEntry.getValue().addIssue(Environment.RIGHT, "FYI:Only processing DB.");
////                }
////                for (TableMirror tm : dbEntry.getValue().getTableMirrors().values()) {
////                    if (tm.isRemove()) {
////                        dbEntry.getValue().getFilteredOut().put(tm.getName(), tm.getRemoveReason());
////                    }
////                }
////            }
////
////            // Remove all the tblMirrors that shouldn't be processed based on config.
////            for (Map.Entry<String, DBMirror> dbEntry : conversion.getDatabases().entrySet()) {
////                dbEntry.getValue().getTableMirrors().values().removeIf(value -> value.isRemove());
////            }
////
////            if (getConfig().getDumpTestData()) {
////                String conversionFileStr = reportOutputDir + System.getProperty("file.separator") + "conversion.yaml";
////                File conversionFile = new File(conversionFileStr);
////                try (FileWriter conversionFileWriter = new FileWriter(conversionFile)) {
////                    String conversionYamlStr = mapper.writeValueAsString(conversion);
////                    conversionFileWriter.write(conversionYamlStr);
////                    log.info("Conversion yaml 'saved' to: " + conversionFile.getPath());
////                } catch (IOException ioe) {
////                    log.error("Problem 'writing' conversion yaml", ioe);
////                }
////                return -99;
////            }
////
////            // GO TIME!!!
////            conversion = runTransfer(conversion);
////
////            // Actions
////        } else {
////            setupError = Boolean.TRUE;
////        }
//
//
////        } else {
////            conversion = runTransfer(conversion);
////        }
//
//        // Remove the abstract environments from config before reporting output.
////        getConfig().getClusters().remove(Environment.TRANSFER);
////        getConfig().getClusters().remove(Environment.SHADOW);
//
////        if (!setupError) {
////            for (String database : getConfig().getDatabases()) {
////
////                String dbReportOutputFile = reportOutputDir + System.getProperty("file.separator") + database + "_hms-mirror";
////                String dbLeftExecuteFile = reportOutputDir + System.getProperty("file.separator") + database + "_LEFT_execute.sql";
////                String dbLeftCleanUpFile = reportOutputDir + System.getProperty("file.separator") + database + "_LEFT_CleanUp_execute.sql";
////                String dbRightExecuteFile = reportOutputDir + System.getProperty("file.separator") + database + "_RIGHT_execute.sql";
////                String dbRightCleanUpFile = reportOutputDir + System.getProperty("file.separator") + database + "_RIGHT_CleanUp_execute.sql";
////                String dbRunbookFile = reportOutputDir + System.getProperty("file.separator") + database + "_runbook.md";
////
////                try {
////                    // Output directory maps
////                    Boolean dcLeft = Boolean.FALSE;
////                    Boolean dcRight = Boolean.FALSE;
////
////                    if (getConfig().canDeriveDistcpPlan()) {
////                        try {
////                            Environment[] environments = null;
////                            switch (getConfig().getDataStrategy()) {
////
////                                case DUMP:
////                                case STORAGE_MIGRATION:
////                                    environments = new Environment[]{Environment.LEFT};
////                                    break;
////                                default:
////                                    environments = new Environment[]{Environment.LEFT, Environment.RIGHT};
////                                    break;
////                            }
////
////                            for (Environment distcpEnv : environments) {
////                                Boolean dcFound = Boolean.FALSE;
////
////                                StringBuilder distcpWorkbookSb = new StringBuilder();
////                                StringBuilder distcpScriptSb = new StringBuilder();
////
////                                distcpScriptSb.append("#!/usr/bin/env sh").append("\n");
////                                distcpScriptSb.append("\n");
////                                distcpScriptSb.append("# 1. Copy the source '*_distcp_source.txt' files to the distributed filesystem.").append("\n");
////                                distcpScriptSb.append("# 2. Export an env var 'HCFS_BASE_DIR' that represents where these files where placed.").append("\n");
////                                distcpScriptSb.append("#      NOTE: ${HCFS_BASE_DIR} must be available to the user running 'distcp'").append("\n");
////                                distcpScriptSb.append("# 3. Export an env var 'DISTCP_OPTS' with any special settings needed to run the job.").append("\n");
////                                distcpScriptSb.append("#      For large jobs, you may need to adjust memory settings.").append("\n");
////                                distcpScriptSb.append("# 4. Run the following in an order or framework that is appropriate for your environment.").append("\n");
////                                distcpScriptSb.append("#       These aren't necessarily expected to run in this shell script as is in production.").append("\n");
////                                distcpScriptSb.append("\n");
////                                distcpScriptSb.append("\n");
////                                distcpScriptSb.append("if [ -z ${HCFS_BASE_DIR+x} ]; then").append("\n");
////                                distcpScriptSb.append("  echo \"HCFS_BASE_DIR is unset\"").append("\n");
////                                distcpScriptSb.append("  echo \"What is the 'HCFS_BASE_DIR':\"").append("\n");
////                                distcpScriptSb.append("  read HCFS_BASE_DIR").append("\n");
////                                distcpScriptSb.append("  echo \"HCFS_BASE_DIR is set to '$HCFS_BASE_DIR'\"").append("\n");
////                                distcpScriptSb.append("else").append("\n");
////                                distcpScriptSb.append("  echo \"HCFS_BASE_DIR is set to '$HCFS_BASE_DIR'\"").append("\n");
////                                distcpScriptSb.append("fi").append("\n");
////                                distcpScriptSb.append("\n");
////                                distcpScriptSb.append("echo \"Creating HCFS directory: $HCFS_BASE_DIR\"").append("\n");
////                                distcpScriptSb.append("hdfs dfs -mkdir -p $HCFS_BASE_DIR").append("\n");
////                                distcpScriptSb.append("\n");
////
////                                // WARNING ABOUT 'distcp' and 'table alignment'
////                                distcpWorkbookSb.append("## WARNING\n");
////                                distcpWorkbookSb.append(MessageCode.RDL_DC_WARNING_TABLE_ALIGNMENT.getDesc()).append("\n\n");
////
////                                distcpWorkbookSb.append("| Database | Target | Sources |\n");
////                                distcpWorkbookSb.append("|:---|:---|:---|\n");
////
////                                FileWriter distcpSourceFW = null;
////                                for (Map.Entry<String, Map<String, Set<String>>> entry :
////                                        getConfig().getTranslator().buildDistcpList(database, distcpEnv, 1).entrySet()) {
////
////                                    distcpWorkbookSb.append("| " + entry.getKey() + " | | |\n");
////                                    Map<String, Set<String>> value = entry.getValue();
////                                    int i = 1;
////                                    for (Map.Entry<String, Set<String>> dbMap : value.entrySet()) {
////                                        String distcpSourceFile = entry.getKey() + "_" + distcpEnv.toString() + "_" + i++ + "_distcp_source.txt";
////                                        String distcpSourceFileFull = reportOutputDir + System.getProperty("file.separator") + distcpSourceFile;
////                                        distcpSourceFW = new FileWriter(distcpSourceFileFull);
////
////                                        StringBuilder line = new StringBuilder();
////                                        line.append("| | ").append(dbMap.getKey()).append(" | ");
////
////                                        for (String source : dbMap.getValue()) {
////                                            line.append(source).append("<br>");
////                                            distcpSourceFW.append(source).append("\n");
////                                        }
////                                        line.append(" | ").append("\n");
////                                        distcpWorkbookSb.append(line);
////
////                                        distcpScriptSb.append("\n");
////                                        distcpScriptSb.append("echo \"Copying 'distcp' source file to $HCFS_BASE_DIR\"").append("\n");
////                                        distcpScriptSb.append("\n");
////                                        distcpScriptSb.append("hdfs dfs -copyFromLocal -f " + distcpSourceFile + " ${HCFS_BASE_DIR}").append("\n");
////                                        distcpScriptSb.append("\n");
////                                        distcpScriptSb.append("echo \"Running 'distcp'\"").append("\n");
////                                        distcpScriptSb.append("hadoop distcp ${DISTCP_OPTS} -f ${HCFS_BASE_DIR}/" + distcpSourceFile + " " +
////                                                dbMap.getKey() + "\n").append("\n");
////
////                                        distcpSourceFW.close();
////
////                                        dcFound = Boolean.TRUE;
////                                    }
////                                }
////
////                                if (dcFound) {
////                                    // Set flags for report and workplan
////                                    switch (distcpEnv) {
////                                        case LEFT:
////                                            dcLeft = Boolean.TRUE;
////                                            break;
////                                        case RIGHT:
////                                            dcRight = Boolean.TRUE;
////                                            break;
////                                    }
////
////                                    String distcpWorkbookFile = reportOutputDir + System.getProperty("file.separator") + database +
////                                            "_" + distcpEnv + "_distcp_workbook.md";
////                                    String distcpScriptFile = reportOutputDir + System.getProperty("file.separator") + database +
////                                            "_" + distcpEnv + "_distcp_script.sh";
////
////                                    FileWriter distcpWorkbookFW = new FileWriter(distcpWorkbookFile);
////                                    FileWriter distcpScriptFW = new FileWriter(distcpScriptFile);
////
////                                    distcpScriptFW.write(distcpScriptSb.toString());
////                                    distcpWorkbookFW.write(distcpWorkbookSb.toString());
////
////                                    distcpScriptFW.close();
////                                    distcpWorkbookFW.close();
////                                }
////                            }
////                        } catch (IOException ioe) {
////                            log.error("Issue writing distcp workbook", ioe);
////                        }
////                    }
////
////
////                    FileWriter runbookFile = new FileWriter(dbRunbookFile);
////                    runbookFile.write("# Runbook for database: " + database);
////                    runbookFile.write("\n\nYou'll find the **run report** in the file:\n\n`" + dbReportOutputFile + ".md|html` " +
////                            "\n\nThis file includes details about the configuration at the time this was run and the " +
////                            "output/actions on each table in the database that was included.\n\n");
////                    runbookFile.write("## Steps\n\n");
////                    if (getConfig().isExecute()) {
////                        runbookFile.write("Execute was **ON**, so many of the scripts have been run already.  Verify status " +
////                                "in the above report.  `distcp` actions (if requested/applicable) need to be run manually. " +
////                                "Some cleanup scripts may have been run if no `distcp` actions were requested.\n\n");
////                        if (getConfig().getCluster(Environment.RIGHT).getHiveServer2() != null) {
////                            if (getConfig().getCluster(Environment.RIGHT).getHiveServer2().isDisconnected()) {
////                                runbookFile.write("Process ran with RIGHT environment 'disconnected'.  All RIGHT scripts will need to be run manually.\n\n");
////                            }
////                        }
////                    } else {
////                        runbookFile.write("Execute was **OFF**.  All actions will need to be run manually. See below steps.\n\n");
////                    }
////                    int step = 1;
////                    FileWriter reportFile = new FileWriter(dbReportOutputFile + ".md");
////                    String mdReportStr = conversion.toReport(config, database);
////
////
////                    File dbYamlFile = new File(dbReportOutputFile + ".yaml");
////                    FileWriter dbYamlFileWriter = new FileWriter(dbYamlFile);
////
////                    DBMirror yamlDb = conversion.getDatabase(database);
////                    Map<PhaseState, Integer> phaseSummaryMap = yamlDb.getPhaseSummary();
////                    if (phaseSummaryMap.containsKey(PhaseState.ERROR)) {
////                        Integer errCount = phaseSummaryMap.get(PhaseState.ERROR);
////                        rtn += errCount;
////                    }
////
////                    String dbYamlStr = mapper.writeValueAsString(yamlDb);
////                    try {
////                        dbYamlFileWriter.write(dbYamlStr);
////                        log.info("Database (" + database + ") yaml 'saved' to: " + dbYamlFile.getPath());
////                    } catch (IOException ioe) {
////                        log.error("Problem 'writing' database yaml", ioe);
////                    } finally {
////                        dbYamlFileWriter.close();
////                    }
////
////                    reportFile.write(mdReportStr);
////                    reportFile.flush();
////                    reportFile.close();
////                    // Convert to HTML
////                    List<Extension> extensions = Arrays.asList(TablesExtension.create(), YamlFrontMatterExtension.create());
////
////                    org.commonmark.parser.Parser parser = org.commonmark.parser.Parser.builder().extensions(extensions).build();
////                    Node document = parser.parse(mdReportStr);
////                    HtmlRenderer renderer = HtmlRenderer.builder().extensions(extensions).build();
////                    String htmlReportStr = renderer.render(document);  // "<p>This is <em>Sparta</em></p>\n"
////                    reportFile = new FileWriter(dbReportOutputFile + ".html");
////                    reportFile.write(htmlReportStr);
////                    reportFile.close();
////
////                    log.info("Status Report of 'hms-mirror' is here: " + dbReportOutputFile + ".md|html");
////
////                    String les = conversion.executeSql(Environment.LEFT, database);
////                    if (les != null) {
////                        FileWriter leftExecOutput = new FileWriter(dbLeftExecuteFile);
////                        leftExecOutput.write(les);
////                        leftExecOutput.close();
////                        log.info("LEFT Execution Script is here: " + dbLeftExecuteFile);
////                        runbookFile.write(step++ + ". **LEFT** clusters SQL script. ");
////                        if (getConfig().isExecute()) {
////                            runbookFile.write(" (Has been executed already, check report file details)");
////                        } else {
////                            runbookFile.write("(Has NOT been executed yet)");
////                        }
////                        runbookFile.write("\n");
////                    }
////
////                    if (dcLeft) {
////                        runbookFile.write(step++ + ". **LEFT** cluster `distcp` actions.  Needs to be performed manually.  Use 'distcp' report/template.");
////                        runbookFile.write("\n");
////                    }
////
////                    String res = conversion.executeSql(Environment.RIGHT, database);
////                    if (res != null) {
////                        FileWriter rightExecOutput = new FileWriter(dbRightExecuteFile);
////                        rightExecOutput.write(res);
////                        rightExecOutput.close();
////                        log.info("RIGHT Execution Script is here: " + dbRightExecuteFile);
////                        runbookFile.write(step++ + ". **RIGHT** clusters SQL script. ");
////                        if (getConfig().isExecute()) {
////                            if (!getConfig().getCluster(Environment.RIGHT).getHiveServer2().isDisconnected()) {
////                                runbookFile.write(" (Has been executed already, check report file details)");
////                            } else {
////                                runbookFile.write(" (Has NOT been executed because the environment is NOT connected.  Review and run scripts manually.)");
////                            }
////                        } else {
////                            runbookFile.write("(Has NOT been executed yet)");
////                        }
////                        runbookFile.write("\n");
////                    }
////
////                    if (dcRight) {
////                        runbookFile.write(step++ + ". **RIGHT** cluster `distcp` actions.  Needs to be performed manually.  Use 'distcp' report/template.");
////                        runbookFile.write("\n");
////                    }
////
////                    String lcu = conversion.executeCleanUpSql(Environment.LEFT, database);
////                    if (lcu != null) {
////                        FileWriter leftCleanUpOutput = new FileWriter(dbLeftCleanUpFile);
////                        leftCleanUpOutput.write(lcu);
////                        leftCleanUpOutput.close();
////                        log.info("LEFT CleanUp Execution Script is here: " + dbLeftCleanUpFile);
////                        runbookFile.write(step++ + ". **LEFT** clusters CLEANUP SQL script. ");
////                        runbookFile.write("(Has NOT been executed yet)");
////                        runbookFile.write("\n");
////                    }
////
////                    String rcu = conversion.executeCleanUpSql(Environment.RIGHT, database);
////                    if (rcu != null) {
////                        FileWriter rightCleanUpOutput = new FileWriter(dbRightCleanUpFile);
////                        rightCleanUpOutput.write(rcu);
////                        rightCleanUpOutput.close();
////                        log.info("RIGHT CleanUp Execution Script is here: " + dbRightCleanUpFile);
////                        runbookFile.write(step++ + ". **RIGHT** clusters CLEANUP SQL script. ");
////                        runbookFile.write("(Has NOT been executed yet)");
////                        runbookFile.write("\n");
////                    }
////                    log.info("Runbook here: " + dbRunbookFile);
////                    runbookFile.close();
////                } catch (IOException ioe) {
////                    log.error("Issue writing report for: " + database, ioe);
////                }
////            }
////        }
////        Date endTime = new Date();
//        DecimalFormat decf = new DecimalFormat("#.###");
//        decf.setRoundingMode(RoundingMode.CEILING);
//
//        log.info("HMS-Mirror: Completed in " +
//                decf.format((Double) ((endTime.getTime() - startTime.getTime()) / (double) 1000)) + " secs");
////        reporter.stop();
////        reporter.refresh(Boolean.TRUE);
//        return rtn;
//    }

//    public CommandLine getCommandLine(String[] args) {
//        Options options = getOptions();
//
//        CommandLineParser parser = new PosixParser();
//        CommandLine cmd = null;
//
//        try {
//            cmd = parser.parse(options, args);
//        } catch (ParseException pe) {
//            System.out.println("Missing Arguments: " + pe.getMessage());
//            HelpFormatter formatter = new HelpFormatter();
//            String cmdline = ReportingConf.substituteVariablesFromManifest("hms-mirror <options> \nversion:${HMS-Mirror-Version}");
//            formatter.printHelp(100, cmdline, "Hive Metastore Migration Utility", options,
//                    "\nVisit https://github.com/cloudera-labs/hms-mirror/blob/main/README.md for detailed docs.");
////            formatter.printHelp(cmdline, options);
//            throw new RuntimeException(pe);
//        }
//
//        if (cmd.hasOption("h")) {
//            HelpFormatter formatter = new HelpFormatter();
//            String cmdline = ReportingConf.substituteVariablesFromManifest("hms-mirror <options> \nversion:${HMS-Mirror-Version}");
//            formatter.printHelp(100, cmdline, "Hive Metastore Migration Utility", options,
//                    "\nVisit https://github.com/cloudera-labs/hms-mirror/blob/main/README.md for detailed docs");
////            formatter.printHelp(cmdline, options);
//            System.exit(0);
//        }
//
//        return cmd;
//    }

//    private Config getConfig() {
//        if (config == null) {
//            config = ConnectionPoolService.getInstance().getConfig();
//        }
//        return config;
//    }
//
//    public Conversion getConversion() {
//        return conversion;
//    }
//
//    public String getDateMarker() {
//        if (dateMarker == null) {
//            DateFormat df = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss");
//            dateMarker = df.format(new Date());
//        }
//        return dateMarker;
//    }

//    public void setDateMarker(String dateMarker) {
//        this.dateMarker = dateMarker;
//    }

//    private Options getOptions() {
//        // create Options object
//        Options options = new Options();
//
//        Option quietOutput = new Option("q", "quiet", false,
//                "Reduce screen reporting output.  Good for background processes with output redirects to a file");
//        quietOutput.setOptionalArg(Boolean.FALSE);
//        quietOutput.setRequired(Boolean.FALSE);
//        options.addOption(quietOutput);
//
//        Option resetTarget = new Option("rr", "reset-right", false,
//                "Use this for testing to remove the database on the RIGHT using CASCADE.");
//        resetTarget.setRequired(Boolean.FALSE);
//        options.addOption(resetTarget);
//
//        Option resetToDefaultLocation = new Option("rdl", "reset-to-default-location", false,
//                "Strip 'LOCATION' from all target cluster definitions.  This will allow the system defaults " +
//                        "to take over and define the location of the new datasets.");
//        resetToDefaultLocation.setRequired(Boolean.FALSE);
//        options.addOption(resetToDefaultLocation);
//
//        Option skipLegacyTranslation = new Option("slt", "skip-legacy-translation", false,
//                "Skip Schema Upgrades and Serde Translations");
//        skipLegacyTranslation.setRequired(Boolean.FALSE);
//        options.addOption(skipLegacyTranslation);
//
//        Option flipOption = new Option("f", "flip", false,
//                "Flip the definitions for LEFT and RIGHT.  Allows the same config to be used in reverse.");
//        flipOption.setOptionalArg(Boolean.FALSE);
//        flipOption.setRequired(Boolean.FALSE);
//        options.addOption(flipOption);
//
//        Option smDistCpOption = new Option("dc", "distcp", false,
//                "Build the 'distcp' workplans.  Optional argument (PULL, PUSH) to define which cluster is running " +
//                        "the distcp commands.  Default is PULL.");
//        smDistCpOption.setArgs(1);
//        smDistCpOption.setOptionalArg(Boolean.TRUE);
//        smDistCpOption.setArgName("flow-direction default:PULL");
//        smDistCpOption.setRequired(Boolean.FALSE);
//        options.addOption(smDistCpOption);
//
//        Option metadataStage = new Option("d", "data-strategy", true,
//                "Specify how the data will follow the schema. " + Arrays.deepToString(DataStrategyEnum.visibleValues()));
//        metadataStage.setOptionalArg(Boolean.TRUE);
//        metadataStage.setArgName("strategy");
//        metadataStage.setRequired(Boolean.FALSE);
//        options.addOption(metadataStage);
//
//        Option dumpSource = new Option("ds", "dump-source", true,
//                "Specify which 'cluster' is the source for the DUMP strategy (LEFT|RIGHT). ");
//        dumpSource.setOptionalArg(Boolean.TRUE);
//        dumpSource.setArgName("source");
//        dumpSource.setRequired(Boolean.FALSE);
//        options.addOption(dumpSource);
//
//        Option propertyOverrides = new Option("po", "property-overrides", true,
//                "Comma separated key=value pairs of Hive properties you wish to set/override.");
//        propertyOverrides.setArgName("key=value");
//        propertyOverrides.setRequired(Boolean.FALSE);
//        propertyOverrides.setValueSeparator(',');
//        propertyOverrides.setArgs(100);
//        options.addOption(propertyOverrides);
//
//        Option propertyLeftOverrides = new Option("pol", "property-overrides-left", true,
//                "Comma separated key=value pairs of Hive properties you wish to set/override for LEFT cluster.");
//        propertyLeftOverrides.setArgName("key=value");
//        propertyLeftOverrides.setRequired(Boolean.FALSE);
//        propertyLeftOverrides.setValueSeparator(',');
//        propertyLeftOverrides.setArgs(100);
//        options.addOption(propertyLeftOverrides);
//
//        Option propertyRightOverrides = new Option("por", "property-overrides-right", true,
//                "Comma separated key=value pairs of Hive properties you wish to set/override for RIGHT cluster.");
//        propertyRightOverrides.setArgName("key=value");
//        propertyRightOverrides.setRequired(Boolean.FALSE);
//        propertyRightOverrides.setValueSeparator(',');
//        propertyRightOverrides.setArgs(100);
//        options.addOption(propertyRightOverrides);
//
//        OptionGroup optimizationsGroup = new OptionGroup();
//        optimizationsGroup.setRequired(Boolean.FALSE);
//
//        Option skipStatsCollectionOption = new Option("ssc", "skip-stats-collection", false,
//                "Skip collecting basic FS stats for a table.  This WILL affect the optimizer and our ability to " +
//                        "determine the best strategy for moving data.");
//        skipStatsCollectionOption.setRequired(Boolean.FALSE);
//        optimizationsGroup.addOption(skipStatsCollectionOption);
//
//        Option skipOptimizationsOption = new Option("so", "skip-optimizations", false,
//                "Skip any optimizations during data movement, like dynamic sorting or distribute by");
//        skipOptimizationsOption.setRequired(Boolean.FALSE);
//        optimizationsGroup.addOption(skipOptimizationsOption);
//
//        Option sdpiOption = new Option("sdpi", "sort-dynamic-partition-inserts", false,
//                "Used to set `hive.optimize.sort.dynamic.partition` in TEZ for optimal partition inserts.  " +
//                        "When not specified, will use prescriptive sorting by adding 'DISTRIBUTE BY' to transfer SQL. " +
//                        "default: false");
//        sdpiOption.setRequired(Boolean.FALSE);
//        optimizationsGroup.addOption(sdpiOption);
//
//        Option autoTuneOption = new Option("at", "auto-tune", false,
//                "Auto-tune Session Settings for SELECT's and DISTRIBUTION for Partition INSERT's.");
//        autoTuneOption.setRequired(Boolean.FALSE);
//        optimizationsGroup.addOption(autoTuneOption);
//
//        options.addOptionGroup(optimizationsGroup);
//
//        Option compressTextOutputOption = new Option("cto", "compress-text-output", false,
//                "Data movement (SQL/STORAGE_MIGRATION) of TEXT based file formats will be compressed in the new " +
//                        "table.");
//        compressTextOutputOption.setRequired(Boolean.FALSE);
//        options.addOption(compressTextOutputOption);
//
//        Option icebergVersionOption = new Option("iv", "iceberg-version", true,
//                "Specify the Iceberg Version to use.  Specify 1 or 2.  Default is 2.");
//        icebergVersionOption.setOptionalArg(Boolean.TRUE);
//        icebergVersionOption.setArgName("version");
//        icebergVersionOption.setRequired(Boolean.FALSE);
//        options.addOption(icebergVersionOption);
//
//        Option icebergTablePropertyOverrides = new Option("itpo", "iceberg-table-property-overrides", true,
//                "Comma separated key=value pairs of Iceberg Table Properties to set/override.");
//        icebergTablePropertyOverrides.setArgName("key=value");
//        icebergTablePropertyOverrides.setRequired(Boolean.FALSE);
//        icebergTablePropertyOverrides.setValueSeparator(',');
//        icebergTablePropertyOverrides.setArgs(100);
//        options.addOption(icebergTablePropertyOverrides);
//
//        Option createIfNotExistsOption = new Option("cine", "create-if-not-exist", false,
//                "CREATE table/partition statements will be adjusted to include 'IF NOT EXISTS'.  This will ensure " +
//                        "all remaining sql statements will be run.  This can be used to sync partition definitions for existing tables.");
//        createIfNotExistsOption.setRequired(Boolean.FALSE);
//        options.addOption(createIfNotExistsOption);
//
//        OptionGroup testDataOptionGroup = new OptionGroup();
//
//        Option dumpTestDataOption = new Option("dtd", "dump-test-data", false,
//                "Used to dump a data set that can be feed into the process for testing.");
//        dumpTestDataOption.setRequired(Boolean.FALSE);
//        testDataOptionGroup.addOption(dumpTestDataOption);
//
//        Option loadTestDataOption = new Option("ltd", "load-test-data", true,
//                "Use the data saved by the `-dtd` option to test the process.");
//        loadTestDataOption.setOptionalArg(Boolean.TRUE);
//        loadTestDataOption.setArgName("file");
//        // Adding to dbGroup because it overrides those values.
////        testDataOptionGroup.addOption(loadTestDataOption);
//
//        options.addOptionGroup(testDataOptionGroup);
//
//        Option forceExternalLocationOption = new Option("fel", "force-external-location", false,
//                "Under some conditions, the LOCATION element for EXTERNAL tables is removed (ie: -rdl).  " +
//                        "In which case we rely on the settings of the database definition to control the " +
//                        "EXTERNAL table data location.  But for some older Hive versions, the LOCATION element in " +
//                        "the database is NOT honored.  Even when the database LOCATION is set, the EXTERNAL table LOCATION " +
//                        "defaults to the system wide warehouse settings.  This flag will ensure the LOCATION element " +
//                        "remains in the CREATE definition of the table to force it's location.");
//        forceExternalLocationOption.setRequired(Boolean.FALSE);
//        options.addOption(forceExternalLocationOption);
//
//        Option glblLocationMapOption = new Option("glm", "global-location-map", true,
//                "Comma separated key=value pairs of Locations to Map. IE: /myorig/data/finance=/data/ec/finance. " +
//                        "This reviews 'EXTERNAL' table locations for the path '/myorig/data/finance' and replaces it " +
//                        "with '/data/ec/finance'.  Option can be used alone or with -rdl. Only applies to 'EXTERNAL' tables " +
//                        "and if the tables location doesn't contain one of the supplied maps, it will be translated according " +
//                        "to -rdl rules if -rdl is specified.  If -rdl is not specified, the conversion for that table is skipped. ");
//        glblLocationMapOption.setArgName("key=value");
//        glblLocationMapOption.setRequired(Boolean.FALSE);
//        glblLocationMapOption.setValueSeparator(',');
//        glblLocationMapOption.setArgs(1000);
//        options.addOption(glblLocationMapOption);
//
//        OptionGroup storageOptionsGroup = new OptionGroup();
//        storageOptionsGroup.setRequired(Boolean.FALSE);
//
//        Option intermediateStorageOption = new Option("is", "intermediate-storage", true,
//                "Intermediate Storage used with Data Strategy HYBRID, SQL, EXPORT_IMPORT.  This will change " +
//                        "the way these methods are implemented by using the specified storage location as an " +
//                        "intermediate transfer point between two clusters.  In this case, the cluster do NOT need to " +
//                        "be 'linked'.  Each cluster DOES need to have access to the location and authorization to " +
//                        "interact with the location.  This may mean additional configuration requirements for " +
//                        "'hdfs' to ensure this seamless access.");
//        intermediateStorageOption.setOptionalArg(Boolean.TRUE);
//        intermediateStorageOption.setArgName("storage-path");
//        intermediateStorageOption.setRequired(Boolean.FALSE);
//        storageOptionsGroup.addOption(intermediateStorageOption);
//
//        Option commonStorageOption = new Option("cs", "common-storage", true,
//                "Common Storage used with Data Strategy HYBRID, SQL, EXPORT_IMPORT.  This will change " +
//                        "the way these methods are implemented by using the specified storage location as an " +
//                        "'common' storage point between two clusters.  In this case, the cluster do NOT need to " +
//                        "be 'linked'.  Each cluster DOES need to have access to the location and authorization to " +
//                        "interact with the location.  This may mean additional configuration requirements for " +
//                        "'hdfs' to ensure this seamless access.");
//        commonStorageOption.setOptionalArg(Boolean.TRUE);
//        commonStorageOption.setArgName("storage-path");
//        commonStorageOption.setRequired(Boolean.FALSE);
//        storageOptionsGroup.addOption(commonStorageOption);
//
//        options.addOptionGroup(storageOptionsGroup);
//
//        // External Warehouse Dir
//        Option externalWarehouseDirOption = new Option("ewd", "external-warehouse-directory", true,
//                "The external warehouse directory path.  Should not include the namespace OR the database directory. " +
//                        "This will be used to set the LOCATION database option.");
//        externalWarehouseDirOption.setOptionalArg(Boolean.TRUE);
//        externalWarehouseDirOption.setArgName("path");
//        externalWarehouseDirOption.setRequired(Boolean.FALSE);
//        options.addOption(externalWarehouseDirOption);
//
//        // Warehouse Dir
//        Option warehouseDirOption = new Option("wd", "warehouse-directory", true,
//                "The warehouse directory path.  Should not include the namespace OR the database directory. " +
//                        "This will be used to set the MANAGEDLOCATION database option.");
//        warehouseDirOption.setOptionalArg(Boolean.TRUE);
//        warehouseDirOption.setArgName("path");
//        warehouseDirOption.setRequired(Boolean.FALSE);
//        options.addOption(warehouseDirOption);
//
//        // Migration Options - Only one of these can be selected at a time, but isn't required.
//        OptionGroup migrationOptionsGroup = new OptionGroup();
//        migrationOptionsGroup.setRequired(Boolean.FALSE);
//
//        Option dboOption = new Option("dbo", "database-only", false,
//                "Migrate the Database definitions as they exist from LEFT to RIGHT");
//        dboOption.setRequired(Boolean.FALSE);
//        migrationOptionsGroup.addOption(dboOption);
//
//        Option maoOption = new Option("mao", "migrate-acid-only", false,
//                "Migrate ACID tables ONLY (if strategy allows). Optional: ArtificialBucketThreshold count that will remove " +
//                        "the bucket definition if it's below this.  Use this as a way to remove artificial bucket definitions that " +
//                        "were added 'artificially' in legacy Hive. (default: 2)");
//        maoOption.setArgs(1);
//        maoOption.setOptionalArg(Boolean.TRUE);
//        maoOption.setArgName("bucket-threshold (2)");
//        maoOption.setRequired(Boolean.FALSE);
//        migrationOptionsGroup.addOption(maoOption);
//
//        Option mnnoOption = new Option("mnno", "migrate-non-native-only", false,
//                "Migrate Non-Native tables (if strategy allows). These include table definitions that rely on " +
//                        "external connection to systems like: HBase, Kafka, JDBC");
//        mnnoOption.setRequired(Boolean.FALSE);
//        migrationOptionsGroup.addOption(mnnoOption);
//
//        Option viewOption = new Option("v", "views-only", false,
//                "Process VIEWs ONLY");
//        viewOption.setRequired(false);
//        migrationOptionsGroup.addOption(viewOption);
//
//        options.addOptionGroup(migrationOptionsGroup);
//
//        Option maOption = new Option("ma", "migrate-acid", false,
//                "Migrate ACID tables (if strategy allows). Optional: ArtificialBucketThreshold count that will remove " +
//                        "the bucket definition if it's below this.  Use this as a way to remove artificial bucket definitions that " +
//                        "were added 'artificially' in legacy Hive. (default: 2)");
//        maOption.setArgs(1);
//        maOption.setOptionalArg(Boolean.TRUE);
//        maOption.setArgName("bucket-threshold (2)");
//        maOption.setRequired(Boolean.FALSE);
//        options.addOption(maOption);
//
//        Option daOption = new Option("da", "downgrade-acid", false,
//                "Downgrade ACID tables to EXTERNAL tables with purge.");
//        daOption.setRequired(Boolean.FALSE);
//        options.addOption(daOption);
//
//        Option evaluatePartLocationOption = new Option("epl", "evaluate-partition-location", false,
//                "For SCHEMA_ONLY and DUMP data-strategies, review the partition locations and build " +
//                        "partition metadata calls to create them is they can't be located via 'MSCK'.");
//        evaluatePartLocationOption.setRequired(Boolean.FALSE);
//        options.addOption(evaluatePartLocationOption);
//
//        Option ridOption = new Option("rid", "right-is-disconnected", false,
//                "Don't attempt to connect to the 'right' cluster and run in this mode");
//        ridOption.setRequired(Boolean.FALSE);
//        options.addOption(ridOption);
//
//        Option ipOption = new Option("ip", "in-place", false,
//                "Downgrade ACID tables to EXTERNAL tables with purge.");
//        ipOption.setRequired(Boolean.FALSE);
//        options.addOption(ipOption);
//
//        Option skipLinkTestOption = new Option("slc", "skip-link-check", false,
//                "Skip Link Check. Use when going between or to Cloud Storage to avoid having to configure " +
//                        "hms-mirror with storage credentials and libraries. This does NOT preclude your Hive Server 2 and " +
//                        "compute environment from such requirements.");
//        skipLinkTestOption.setRequired(Boolean.FALSE);
//        options.addOption(skipLinkTestOption);
//
//        // Non Native Migrations
//        Option mnnOption = new Option("mnn", "migrate-non-native", false,
//                "Migrate Non-Native tables (if strategy allows). These include table definitions that rely on " +
//                        "external connection to systems like: HBase, Kafka, JDBC");
//        mnnOption.setArgs(1);
//        mnnOption.setOptionalArg(Boolean.TRUE);
//        mnnOption.setRequired(Boolean.FALSE);
//        options.addOption(mnnOption);
//
//        // TODO: Implement this feature...  If requested.  Needs testings, not complete after other downgrade work.
////        Option replaceOption = new Option("r", "replace", false,
////                "When downgrading an ACID table as its transferred to the 'RIGHT' cluster, this option " +
////                        "will replace the current ACID table on the LEFT cluster with a 'downgraded' table (EXTERNAL). " +
////                        "The option only works with options '-da' and '-cs'.");
////        replaceOption.setRequired(Boolean.FALSE);
////        options.addOption(replaceOption);
//
//        Option syncOption = new Option("s", "sync", false,
//                "For SCHEMA_ONLY, COMMON, and LINKED data strategies.  Drop and Recreate Schema's when different.  " +
//                        "Best to use with RO to ensure table/partition drops don't delete data. When used WITHOUT `-tf` it will " +
//                        "compare all the tables in a database and sync (bi-directional).  Meaning it will DROP tables on the RIGHT " +
//                        "that aren't in the LEFT and ADD tables to the RIGHT that are missing.  When used with `-ro`, table schemas can be updated " +
//                        "by dropping and recreating.  When used with `-tf`, only the tables that match the filter (on both " +
//                        "sides) will be considered.\n When used with HYBRID, SQL, and EXPORT_IMPORT data strategies and ACID tables " +
//                        "are involved, the tables will be dropped and recreated.  The data in this case WILL be dropped and replaced.");
//        syncOption.setRequired(Boolean.FALSE);
//        options.addOption(syncOption);
//
//        Option roOption = new Option("ro", "read-only", false,
//                "For SCHEMA_ONLY, COMMON, and LINKED data strategies set RIGHT table to NOT purge on DROP. " +
//                        "Intended for use with replication distcp strategies and has restrictions about existing DB's " +
//                        "on RIGHT and PATH elements.  To simply NOT set the purge flag for applicable tables, use -np.");
//        roOption.setRequired(Boolean.FALSE);
//        options.addOption(roOption);
//
//        Option npOption = new Option("np", "no-purge", false,
//                "For SCHEMA_ONLY, COMMON, and LINKED data strategies set RIGHT table to NOT purge on DROP");
//        npOption.setRequired(Boolean.FALSE);
//        options.addOption(npOption);
//
//        Option acceptOption = new Option("accept", "accept", false,
//                "Accept ALL confirmations and silence prompts");
//        acceptOption.setRequired(Boolean.FALSE);
//        options.addOption(acceptOption);
//
//        // TODO: Add addition Storage Migration Strategies (current default and only option is SQL)
////        Option translateConfigOption = new Option("t", "translate-config", true,
////                "Translator Configuration File (Experimental)");
////        translateConfigOption.setRequired(Boolean.FALSE);
////        translateConfigOption.setArgName("translate-config-file");
////        options.addOption(translateConfigOption);
//
//        Option outputOption = new Option("o", "output-dir", true,
//                "Output Directory (default: $HOME/.hms-mirror/reports/<yyyy-MM-dd_HH-mm-ss>");
//        outputOption.setRequired(Boolean.FALSE);
//        outputOption.setArgName("outputdir");
//        options.addOption(outputOption);
//
//        Option skipFeaturesOption = new Option("sf", "skip-features", false,
//                "Skip Features evaluation.");
//        skipFeaturesOption.setRequired(Boolean.FALSE);
//        options.addOption(skipFeaturesOption);
//
//        Option executeOption = new Option("e", "execute", false,
//                "Execute actions request, without this flag the process is a dry-run.");
//        executeOption.setRequired(Boolean.FALSE);
//        options.addOption(executeOption);
//
//        Option asmOption = new Option("asm", "avro-schema-migration", false,
//                "Migrate AVRO Schema Files referenced in TBLPROPERTIES by 'avro.schema.url'.  Without migration " +
//                        "it is expected that the file will exist on the other cluster and match the 'url' defined in the " +
//                        "schema DDL.\nIf it's not present, schema creation will FAIL.\nSpecifying this option REQUIRES the " +
//                        "LEFT and RIGHT cluster to be LINKED.\nSee docs: https://github.com/cloudera-labs/hms-mirror#linking-clusters-storage-layers");
//        asmOption.setRequired(Boolean.FALSE);
//        options.addOption(asmOption);
//
//        Option transferOwnershipOption = new Option("to", "transfer-ownership", false,
//                "If available (supported) on LEFT cluster, extract and transfer the tables owner to the " +
//                        "RIGHT cluster. Note: This will make an 'exta' SQL call on the LEFT cluster to determine " +
//                        "the ownership.  This won't be supported on CDH 5 and some other legacy Hive platforms. " +
//                        "Beware the cost of this extra call for EVERY table, as it may slow down the process for " +
//                        "a large volume of tables.");
//        transferOwnershipOption.setRequired(Boolean.FALSE);
//        options.addOption(transferOwnershipOption);
//
//        OptionGroup dbAdjustOptionGroup = new OptionGroup();
//
//        Option dbPrefixOption = new Option("dbp", "db-prefix", true,
//                "Optional: A prefix to add to the RIGHT cluster DB Name. Usually used for testing.");
//        dbPrefixOption.setRequired(Boolean.FALSE);
//        dbPrefixOption.setArgName("prefix");
//        dbAdjustOptionGroup.addOption(dbPrefixOption);
//
//        Option dbRenameOption = new Option("dbr", "db-rename", true,
//                "Optional: Rename target db to ...  This option is only valid when '1' database is listed in `-db`.");
//        dbRenameOption.setRequired(Boolean.FALSE);
//        dbRenameOption.setArgName("rename");
//        dbAdjustOptionGroup.addOption(dbRenameOption);
//
//        options.addOptionGroup(dbAdjustOptionGroup);
//
//        Option storageMigrationNamespaceOption = new Option("smn", "storage-migration-namespace", true,
//                "Optional: Used with the 'data strategy STORAGE_MIGRATION to specify the target namespace.");
//        storageMigrationNamespaceOption.setRequired(Boolean.FALSE);
//        storageMigrationNamespaceOption.setArgName("namespace");
//        options.addOption(storageMigrationNamespaceOption);
//
////        Option storageMigrationStrategyOption = new Option("sms", "storage-migration-strategy", true,
////                "Optional: Used with the 'data strategy' STORAGE_MIGRATION to specify the technique used to migration.  " +
////                        "Options are: [SQL,EXPORT_IMPORT,HYBRID]. Default is SQL");
////        storageMigrationStrategyOption.setRequired(Boolean.FALSE);
////        storageMigrationStrategyOption.setArgName("Storage Migration Strategy");
////        options.addOption(storageMigrationStrategyOption);
//
//        Option dbOption = new Option("db", "database", true,
//                "Comma separated list of Databases (upto 100).");
//        dbOption.setValueSeparator(',');
//        dbOption.setArgName("databases");
//        dbOption.setArgs(100);
//
//        Option dbRegExOption = new Option("dbRegEx", "database-regex", true,
//                "RegEx of Database to include in process.");
//        dbRegExOption.setRequired(Boolean.FALSE);
//        dbRegExOption.setArgName("regex");
//
//        Option helpOption = new Option("h", "help", false,
//                "Help");
//        helpOption.setRequired(Boolean.FALSE);
//
//        Option pwOption = new Option("p", "password", true,
//                "Used this in conjunction with '-pkey' to generate the encrypted password that you'll add to the configs for the JDBC connections.");
//        pwOption.setRequired(Boolean.FALSE);
//        pwOption.setArgName("password");
//
//        Option decryptPWOption = new Option("dp", "decrypt-password", true,
//                "Used this in conjunction with '-pkey' to decrypt the generated passcode from `-p`.");
//        decryptPWOption.setRequired(Boolean.FALSE);
//        decryptPWOption.setArgName("encrypted-password");
//
//        Option replayOption = new Option("replay", "replay", true,
//                "Use to replay process from the report output.");
//        replayOption.setRequired(Boolean.FALSE);
//        replayOption.setArgName("report-directory");
//
//        Option setupOption = new Option("su", "setup", false,
//                "Setup a default configuration file through a series of questions");
//        setupOption.setRequired(Boolean.FALSE);
//
//        Option pKeyOption = new Option("pkey", "password-key", true,
//                "The key used to encrypt / decrypt the cluster jdbc passwords.  If not present, the passwords will be processed as is (clear text) from the config file.");
//        pKeyOption.setRequired(false);
//        pKeyOption.setArgName("password-key");
//        options.addOption(pKeyOption);
//
//        OptionGroup dbGroup = new OptionGroup();
//        dbGroup.addOption(dbOption);
//        dbGroup.addOption(loadTestDataOption);
//        dbGroup.addOption(dbRegExOption);
//        dbGroup.addOption(helpOption);
//        dbGroup.addOption(setupOption);
//        dbGroup.addOption(pwOption);
//        dbGroup.addOption(decryptPWOption);
//        dbGroup.addOption(replayOption);
//        dbGroup.setRequired(Boolean.TRUE);
//        options.addOptionGroup(dbGroup);
//
//        Option sqlOutputOption = new Option("sql", "sql-output", false,
//                "<deprecated>.  This option is no longer required to get SQL out in a report.  That is the default behavior.");
//        sqlOutputOption.setRequired(Boolean.FALSE);
//        options.addOption(sqlOutputOption);
//
//        Option acidPartCountOption = new Option("ap", "acid-partition-count", true,
//                "Set the limit of partitions that the ACID strategy will work with. '-1' means no-limit.");
//        acidPartCountOption.setRequired(Boolean.FALSE);
//        acidPartCountOption.setArgName("limit");
//        options.addOption(acidPartCountOption);
//
//        Option sqlPartCountOption = new Option("sp", "sql-partition-count", true,
//                "Set the limit of partitions that the SQL strategy will work with. '-1' means no-limit.");
//        sqlPartCountOption.setRequired(Boolean.FALSE);
//        sqlPartCountOption.setArgName("limit");
//        options.addOption(sqlPartCountOption);
//
//        Option expImpPartCountOption = new Option("ep", "export-partition-count", true,
//                "Set the limit of partitions that the EXPORT_IMPORT strategy will work with.");
//        expImpPartCountOption.setRequired(Boolean.FALSE);
//        expImpPartCountOption.setArgName("limit");
//        options.addOption(expImpPartCountOption);
//
//        OptionGroup filterGroup = new OptionGroup();
//        filterGroup.setRequired(Boolean.FALSE);
//
//        Option tableFilterOption = new Option("tf", "table-filter", true,
//                "Filter tables (inclusive) with name matching RegEx. Comparison done with 'show tables' " +
//                        "results.  Check case, that's important.  Hive tables are generally stored in LOWERCASE. " +
//                        "Make sure you double-quote the expression on the commandline.");
//        tableFilterOption.setRequired(Boolean.FALSE);
//        tableFilterOption.setArgName("regex");
//        filterGroup.addOption(tableFilterOption);
//
//        Option excludeTableFilterOption = new Option("tef", "table-exclude-filter", true,
//                "Filter tables (excludes) with name matching RegEx. Comparison done with 'show tables' " +
//                        "results.  Check case, that's important.  Hive tables are generally stored in LOWERCASE. " +
//                        "Make sure you double-quote the expression on the commandline.");
//        excludeTableFilterOption.setRequired(Boolean.FALSE);
//        excludeTableFilterOption.setArgName("regex");
//        filterGroup.addOption(excludeTableFilterOption);
//
//        options.addOptionGroup(filterGroup);
//
//        Option tableSizeFilterOption = new Option("tfs", "table-filter-size-limit", true,
//                "Filter tables OUT that are above the indicated size.  Expressed in MB");
//        tableSizeFilterOption.setRequired(Boolean.FALSE);
//        tableSizeFilterOption.setArgName("size MB");
//        options.addOption(tableSizeFilterOption);
//
//        Option tablePartitionCountFilterOption = new Option("tfp", "table-filter-partition-count-limit", true,
//                "Filter partition tables OUT that are have more than specified here. Non Partitioned table aren't " +
//                        "filtered.");
//        tablePartitionCountFilterOption.setRequired(Boolean.FALSE);
//        tablePartitionCountFilterOption.setArgName("partition-count");
//        options.addOption(tablePartitionCountFilterOption);
//
//        Option cfgOption = new Option("cfg", "config", true,
//                "Config with details for the HMS-Mirror.  Default: $HOME/.hms-mirror/cfg/default.yaml");
//        cfgOption.setRequired(false);
//        cfgOption.setArgName("filename");
//        options.addOption(cfgOption);
//
//        return options;
//    }

//    public Boolean getQuiet() {
//        return quiet;
//    }
//
//    public void setQuiet(Boolean quiet) {
//        this.quiet = quiet;
//    }

//    public long go(String[] args) {
//        long returnCode = 0;
//        log.info("===================================================");
//        log.info("Running: hms-mirror " + ReportingConf.substituteVariablesFromManifest("v.${HMS-Mirror-Version}"));
//        log.info("On Java Version: " + System.getProperty("java.version"));
//        log.info(" with commandline parameters: " + String.join(",", args));
//        log.info("===================================================");
//        try {
//            returnCode = init(args);
//            try {
//                if (returnCode == 0)
//                    returnCode = doit();
//            } catch (RuntimeException rte) {
//                System.out.println(rte.getMessage());
//                rte.printStackTrace();
//                if (config != null && getConfig().getErrors().getReturnCode() > 0) {
//                    returnCode = (getConfig().getErrors().getReturnCode() * -1); //MessageCode.returnCode(getConfig().getErrors());
//                } else {
//                    returnCode = -1;
//                }
//            }
//            // Explicitly close the connection pools.
//            if (!getConfig().isLoadingTestData()) {
//                ConnectionPoolService.getInstance().getConnectionPools().close();
//            }
//        } catch (RuntimeException e) {
//            log.error(e.getMessage(), e);
//            System.err.println("=====================================================");
//            System.err.println("Commandline args: " + Arrays.toString(args));
//            System.err.println();
//            log.error("Commandline args: " + Arrays.toString(args));
//            if (config != null) {
//                returnCode = (getConfig().getErrors().getReturnCode() * -1);
//            } else {
//                returnCode = -1;
//            }
//            e.printStackTrace();
//            System.err.println("\nSee log for stack trace ($HOME/.hms-mirror/logs)");
//        } finally {
//            if (config != null) {
//                if (getConfig().getErrors().getReturnCode() != 0) {
//                    System.err.println("******* ERRORS *********");
//                }
//                for (String error : getConfig().getErrors().getMessages()) {
//                    log.error(error);
//                    System.err.println(error);
//                }
//                if (getConfig().getWarnings().getReturnCode() != 0) {
//                    System.err.println("******* WARNINGS *********");
//                }
//                for (String warning : getConfig().getWarnings().getMessages()) {
//                    log.warn(warning);
//                    System.err.println(warning);
//                }
//            }
//        }
//        return returnCode;
//    }

//    public long init(String[] args) {
//        long rtn = 0l;
//        CommandLineOptions cliOptions = new CommandLineOptions();
//        CommandLine cmd = cliOptions.getCommandLine(args);
////        CommandLine cmd = getCommandLine(args);
//
//        if (cmd.hasOption("replay")) {
//            String replayDirectory = cmd.getOptionValue("replay");
//            replay(replayDirectory);
//        } else {
//
//            if (cmd.hasOption("su")) {
//                configFile = System.getProperty("user.home") + System.getProperty("file.separator") + ".hms-mirror/cfg/default.yaml";
//                File defaultCfg = new File(configFile);
//                if (defaultCfg.exists()) {
//                    Scanner scanner = new Scanner(System.in);
//                    System.out.print("Default Config exists.  Proceed with overwrite:(Y/N) ");
//                    String response = scanner.next();
//                    if (response.equalsIgnoreCase("y")) {
//                        Config.setup(configFile);
//                        System.exit(0);
//                    }
//                } else {
//                    Config.setup(configFile);
//                    System.exit(0);
//                }
//            }
//
//            Config config = loadConfig(cmd);
//
//            if (config.hasErrors()) {
//                return config.getErrors().getReturnCode();
//            } else {
//                config.setCommandLineOptions(args);
//            }
//
//            if (cmd.hasOption("reset-right")) {
//                getConfig().setResetRight(Boolean.TRUE);
//                getConfig().setDatabaseOnly(Boolean.TRUE);
//            } else {
//                if (cmd.hasOption("f")) {
//                    getConfig().setFlip(Boolean.TRUE);
//                }
//
//                if (cmd.hasOption("sf")) {
//                    // Skip Features.
//                    getConfig().setSkipFeatures(Boolean.TRUE);
//                }
//
//                if (cmd.hasOption("q")) {
//                    // Skip Features.
////                    this.setQuiet(Boolean.TRUE);
//                }
//
//                if (cmd.hasOption("r")) {
//                    // replace
//                    getConfig().setReplace(Boolean.TRUE);
//                }
//
////        if (cmd.hasOption("t")) {
////            Translator translator = null;
////            File tCfgFile = new File(cmd.getOptionValue("t"));
////            if (!tCfgFile.exists()) {
////                throw new RuntimeException("Couldn't locate translation configuration file: " + cmd.getOptionValue("t"));
////            } else {
////                try {
////                    System.out.println("Using Translation Config: " + cmd.getOptionValue("t"));
////                    String yamlCfgFile = FileUtils.readFileToString(tCfgFile, Charset.forName("UTF-8"));
////                    translator = mapper.readerFor(Translator.class).readValue(yamlCfgFile);
////                    if (translator.validate()) {
////                        getConfig().setTranslator(translator);
////                    } else {
////                        throw new RuntimeException("Translator config can't be validated, check logs.");
////                    }
////                } catch (Throwable t) {
////                    throw new RuntimeException(t);
////                }
////
////            }
////        }
//
//                if (cmd.hasOption("dtd")) {
//                    getConfig().setDumpTestData(Boolean.TRUE);
//                }
//                if (cmd.hasOption("ltd")) {
//                    getConfig().setLoadTestDataFile(cmd.getOptionValue("ltd"));
//                }
//                if (cmd.hasOption("rdl")) {
//                    getConfig().setResetToDefaultLocation(Boolean.TRUE);
//                }
//                if (cmd.hasOption("fel")) {
//                    getConfig().getTranslator().setForceExternalLocation(Boolean.TRUE);
//                }
//                if (cmd.hasOption("slt")) {
//                    getConfig().setSkipLegacyTranslation(Boolean.TRUE);
//                }
//
//                if (cmd.hasOption("ap")) {
//                    getConfig().getMigrateACID().setPartitionLimit(Integer.valueOf(cmd.getOptionValue("ap")));
//                }
//
//                if (cmd.hasOption("sp")) {
//                    getConfig().getHybrid().setSqlPartitionLimit(Integer.valueOf(cmd.getOptionValue("sp")));
//                }
//
//                if (cmd.hasOption("ep")) {
//                    getConfig().getHybrid().setExportImportPartitionLimit(Integer.valueOf(cmd.getOptionValue("ep")));
//                }
//
//                if (cmd.hasOption("dbp")) {
//                    getConfig().setDbPrefix(cmd.getOptionValue("dbp"));
//                }
//
//                if (cmd.hasOption("dbr")) {
//                    getConfig().setDbRename(cmd.getOptionValue("dbr"));
//                }
//
//                if (cmd.hasOption("v")) {
//                    getConfig().getMigrateVIEW().setOn(Boolean.TRUE);
//                }
//
//                if (cmd.hasOption("dbo")) {
//                    getConfig().setDatabaseOnly(Boolean.TRUE);
//                }
//
//                if (cmd.hasOption("epl")) {
//                    getConfig().setEvaluatePartitionLocation(Boolean.TRUE);
//                }
//
//                if (cmd.hasOption("slc")) {
//                    getConfig().setSkipLinkCheck(Boolean.TRUE);
//                }
//
//                if (cmd.hasOption("cine")) {
//                    if (getConfig().getCluster(Environment.LEFT) != null) {
//                        getConfig().getCluster(Environment.LEFT).setCreateIfNotExists(Boolean.TRUE);
//                    }
//                    if (getConfig().getCluster(Environment.RIGHT) != null) {
//                        getConfig().getCluster(Environment.RIGHT).setCreateIfNotExists(Boolean.TRUE);
//                    }
//                }
//                if (cmd.hasOption("ma")) {
//                    getConfig().getMigrateACID().setOn(Boolean.TRUE);
//                    String bucketLimit = cmd.getOptionValue("ma");
//                    if (bucketLimit != null) {
//                        getConfig().getMigrateACID().setArtificialBucketThreshold(Integer.valueOf(bucketLimit));
//                    }
//                }
//
//                if (cmd.hasOption("mao")) {
//                    getConfig().getMigrateACID().setOnly(Boolean.TRUE);
//                    String bucketLimit = cmd.getOptionValue("mao");
//                    if (bucketLimit != null) {
//                        getConfig().getMigrateACID().setArtificialBucketThreshold(Integer.valueOf(bucketLimit));
//                    }
//                }
//
//                if (getConfig().getMigrateACID().isOn()) {
//                    if (cmd.hasOption("da")) {
//                        // Downgrade ACID tables
//                        getConfig().getMigrateACID().setDowngrade(Boolean.TRUE);
//                    }
//                    if (cmd.hasOption("ip")) {
//                        // Downgrade ACID tables inplace
//                        // Only work on LEFT cluster definition.
//                        log.info("Inplace ACID Downgrade");
//                        getConfig().getMigrateACID().setDowngrade(Boolean.TRUE);
//                        getConfig().getMigrateACID().setInplace(Boolean.TRUE);
//                        // For 'in-place' downgrade, only applies to ACID tables.
//                        // Implies `-mao`.
//                        log.info("Only ACID Tables will be looked at since 'ip' was specified.");
//                        getConfig().getMigrateACID().setOnly(Boolean.TRUE);
//                        // Remove RIGHT cluster and enforce mao
//                        log.info("RIGHT Cluster definition will be disconnected if exists since this is a LEFT cluster ONLY operation");
//                        if (null != getConfig().getCluster(Environment.RIGHT).getHiveServer2())
//                            getConfig().getCluster(Environment.RIGHT).getHiveServer2().setDisconnected(Boolean.TRUE);
//                    }
//                }
//
//                if (cmd.hasOption("iv")) {
//                    getConfig().getIcebergConfig().setVersion(Integer.parseInt(cmd.getOptionValue("iv")));
//                }
//
//                if (cmd.hasOption("itpo")) {
//                    // property overrides.
//                    String[] overrides = cmd.getOptionValues("itpo");
//                    if (overrides != null)
//                        getConfig().getIcebergConfig().setPropertyOverridesStr(overrides);
//
//                }
//
//                // Skip Optimizations.
//                if (cmd.hasOption("so")) {
//                    getConfig().getOptimization().setSkip(Boolean.TRUE);
//                }
//
//                if (cmd.hasOption("ssc")) {
//                    getConfig().getOptimization().setSkipStatsCollection(Boolean.TRUE);
//                }
//
//                // Sort Dynamic Partitions
//                if (cmd.hasOption("sdpi")) {
//                    getConfig().getOptimization().setSortDynamicPartitionInserts(Boolean.TRUE);
//                }
//                // AutoTune.
//                if (cmd.hasOption("at")) {
//                    getConfig().getOptimization().setAutoTune(Boolean.TRUE);
//                }
//
//                //Compress TEXT Output.
//                if (cmd.hasOption("cto")) {
//                    getConfig().getOptimization().setCompressTextOutput(Boolean.TRUE);
//                }
//
//                if (cmd.hasOption("po")) {
//                    // property overrides.
//                    String[] overrides = cmd.getOptionValues("po");
//                    if (overrides != null)
//                        getConfig().getOptimization().getOverrides().setPropertyOverridesStr(overrides, Overrides.Side.BOTH);
//                }
//
//                if (cmd.hasOption("pol")) {
//                    // property overrides.
//                    String[] overrides = cmd.getOptionValues("pol");
//                    if (overrides != null)
//                        getConfig().getOptimization().getOverrides().setPropertyOverridesStr(overrides, Overrides.Side.LEFT);
//                }
//
//                if (cmd.hasOption("por")) {
//                    // property overrides.
//                    String[] overrides = cmd.getOptionValues("por");
//                    if (overrides != null)
//                        getConfig().getOptimization().getOverrides().setPropertyOverridesStr(overrides, Overrides.Side.RIGHT);
//                }
//
//                if (cmd.hasOption("mnn")) {
//                    getConfig().setMigratedNonNative(Boolean.TRUE);
//                }
//
//                if (cmd.hasOption("mnno")) {
//                    getConfig().setMigratedNonNative(Boolean.TRUE);
//                }
//
//                // AVRO Schema Migration
//                if (cmd.hasOption("asm")) {
//                    getConfig().setCopyAvroSchemaUrls(Boolean.TRUE);
//                }
//
//                if (cmd.hasOption("to")) {
//                    getConfig().setTransferOwnership(Boolean.TRUE);
//                }
//
//                if (cmd.hasOption("rid")) {
//                    if (null != getConfig().getCluster(Environment.RIGHT).getHiveServer2())
//                        getConfig().getCluster(Environment.RIGHT).getHiveServer2().setDisconnected(Boolean.TRUE);
//                }
//
//                String dataStrategyStr = cmd.getOptionValue("d");
//                // default is SCHEMA_ONLY
//                if (dataStrategyStr != null) {
//                    DataStrategyEnum dataStrategy = DataStrategyEnum.valueOf(dataStrategyStr.toUpperCase());
//                    getConfig().setDataStrategy(dataStrategy);
//                    if (getConfig().getDataStrategy() == DataStrategyEnum.DUMP) {
//                        getConfig().setExecute(Boolean.FALSE); // No Actions.
//                        getConfig().setSync(Boolean.FALSE);
//                        // If a source cluster is specified for the cluster to DUMP from, set it.
//                        if (cmd.hasOption("ds")) {
//                            try {
//                                Environment source = Environment.valueOf(cmd.getOptionValue("ds").toUpperCase());
//                                getConfig().setDumpSource(source);
//                            } catch (RuntimeException re) {
//                                log.error("The `-ds` option should be either: (LEFT|RIGHT). " + cmd.getOptionValue("ds") +
//                                        " is NOT a valid option.");
//                                throw new RuntimeException("The `-ds` option should be either: (LEFT|RIGHT). " + cmd.getOptionValue("ds") +
//                                        " is NOT a valid option.");
//                            }
//                        } else {
//                            getConfig().setDumpSource(Environment.LEFT);
//                        }
//                    }
//                    if (getConfig().getDataStrategy() == DataStrategyEnum.LINKED) {
//                        if (cmd.hasOption("ma") || cmd.hasOption("mao")) {
//                            log.error("Can't LINK ACID tables.  ma|mao options are not valid with LINKED data strategy.");
//                            throw new RuntimeException("Can't LINK ACID tables.  ma|mao options are not valid with LINKED data strategy.");
//                        }
//                    }
//                    if (cmd.hasOption("smn")) {
//                        getConfig().getTransfer().setCommonStorage(cmd.getOptionValue("smn"));
//                    }
//                    if (cmd.hasOption("sms")) {
//                        try {
//                            DataStrategyEnum migrationStrategy = DataStrategyEnum.valueOf(cmd.getOptionValue("sms"));
//                            getConfig().getTransfer().getStorageMigration().setStrategy(migrationStrategy);
//                        } catch (Throwable t) {
//                            log.error("Only SQL, EXPORT_IMPORT, and HYBRID are valid strategies for STORAGE_MIGRATION");
//                            throw new RuntimeException("Only SQL, EXPORT_IMPORT, and HYBRID are valid strategies for STORAGE_MIGRATION");
//                        }
//                    }
//                }
//
//                if (cmd.hasOption("wd")) {
//                    if (getConfig().getTransfer().getWarehouse() == null)
//                        getConfig().getTransfer().setWarehouse(new WarehouseConfig());
//                    String wdStr = cmd.getOptionValue("wd");
//                    // Remove/prevent duplicate namespace config.
//                    if (getConfig().getTransfer().getCommonStorage() != null) {
//                        if (wdStr.startsWith(getConfig().getTransfer().getCommonStorage())) {
//                            wdStr = wdStr.substring(getConfig().getTransfer().getCommonStorage().length());
//                            log.warn("Managed Warehouse Location Modified (stripped duplicate namespace): " + wdStr);
//                        }
//                    }
//                    getConfig().getTransfer().getWarehouse().setManagedDirectory(wdStr);
//                }
//
//                if (cmd.hasOption("ewd")) {
//                    if (getConfig().getTransfer().getWarehouse() == null)
//                        getConfig().getTransfer().setWarehouse(new WarehouseConfig());
//                    String ewdStr = cmd.getOptionValue("ewd");
//                    // Remove/prevent duplicate namespace config.
//                    if (getConfig().getTransfer().getCommonStorage() != null) {
//                        if (ewdStr.startsWith(getConfig().getTransfer().getCommonStorage())) {
//                            ewdStr = ewdStr.substring(getConfig().getTransfer().getCommonStorage().length());
//                            log.warn("External Warehouse Location Modified (stripped duplicate namespace): " + ewdStr);
//                        }
//                    }
//                    getConfig().getTransfer().getWarehouse().setExternalDirectory(ewdStr);
//                }
//
//                // GLOBAL (EXTERNAL) LOCATION MAP
//                if (cmd.hasOption("glm")) {
//                    String[] globalLocMap = cmd.getOptionValues("glm");
//                    if (globalLocMap != null)
//                        getConfig().setGlobalLocationMapKV(globalLocMap);
//                }
//
//                // When the pkey is specified, we assume the config passwords are encrytped and we'll decrypt them before continuing.
//                if (cmd.hasOption("pkey")) {
//                    // Loop through the HiveServer2 Configs and decode the password.
//                    System.out.println("Password Key specified.  Decrypting config password before submitting.");
//
//                    String pkey = cmd.getOptionValue("pkey");
//                    Protect protect = new Protect(pkey);
//
//                    for (Environment env : Environment.values()) {
//                        Cluster cluster = getConfig().getCluster(env);
//                        if (cluster != null) {
//                            HiveServer2Config hiveServer2Config = cluster.getHiveServer2();
//                            // Don't process shadow, transfer clusters.
//                            if (hiveServer2Config != null) {
//                                Properties props = hiveServer2Config.getConnectionProperties();
//                                String password = props.getProperty("password");
//                                if (password != null) {
//                                    try {
//                                        String decryptedPassword = protect.decrypt(password);
//                                        props.put("password", decryptedPassword);
//                                    } catch (Exception e) {
//                                        getConfig().getErrors().set(MessageCode.PASSWORD_DECRYPT_ISSUE.getCode());
//                                    }
//                                }
//                            }
//
//                            DBStore metastoreDirect = cluster.getMetastoreDirect();
//                            if (metastoreDirect != null) {
//                                Properties props = metastoreDirect.getConnectionProperties();
//                                String password = props.getProperty("password");
//                                if (password != null) {
//                                    try {
//                                        String decryptedPassword = protect.decrypt(password);
//                                        props.put("password", decryptedPassword);
//                                    } catch (Exception e) {
//                                        getConfig().getErrors().set(MessageCode.DECRYPTING_PASSWORD_ISSUE.getCode());
//                                    }
//                                }
//                            }
//
//                        }
//                    }
//                    if (getConfig().getErrors().getReturnCode() > 0)
//                        return getConfig().getErrors().getReturnCode();
//                }
//
//                // To keep the connections and remainder of the processing in place, set the env for the cluster
//                //   to the abstract name.
//                Set<Environment> environmentSet = new HashSet<>();
//                environmentSet.add(Environment.LEFT);
//                environmentSet.add(Environment.RIGHT);
//                for (Environment lenv : environmentSet) {
//                    getConfig().getCluster(lenv).setEnvironment(lenv);
//                }
//
//                // Get intermediate Storage Location
//                if (cmd.hasOption("is")) {
//                    getConfig().getTransfer().setIntermediateStorage(cmd.getOptionValue("is"));
//                    // This usually means an on-prem to cloud migration, which should be a PUSH data flow for distcp from the
//                    // LEFT and PULL from the RIGHT.
//                    getConfig().getTransfer().getStorageMigration().setDataFlow(DistcpFlow.PUSH_PULL);
//                }
//
//                // Get intermediate Storage Location
//                if (cmd.hasOption("cs")) {
//                    getConfig().getTransfer().setCommonStorage(cmd.getOptionValue("cs"));
//                    // This usually means an on-prem to cloud migration, which should be a PUSH data flow for distcp.
//                    getConfig().getTransfer().getStorageMigration().setDataFlow(DistcpFlow.PUSH);
//                }
//
//                // Set this after the is and cd checks.  Those will set default movement, but you can override here.
//                if (cmd.hasOption("dc")) {
//                    getConfig().getTransfer().getStorageMigration().setDistcp(Boolean.TRUE);
//                    String flowStr = cmd.getOptionValue("dc");
//                    if (flowStr != null) {
//                        try {
//                            DistcpFlow flow = DistcpFlow.valueOf(flowStr.toUpperCase(Locale.ROOT));
//                            getConfig().getTransfer().getStorageMigration().setDataFlow(flow);
//                        } catch (IllegalArgumentException iae) {
//                            throw new RuntimeException("Optional argument for `distcp` is invalid. Valid values: " +
//                                    Arrays.toString(DistcpFlow.values()), iae);
//                        }
//                    }
//                }
//
//                if (cmd.hasOption("ro")) {
//                    switch (getConfig().getDataStrategy()) {
//                        case SCHEMA_ONLY:
//                        case LINKED:
//                        case COMMON:
//                        case SQL:
//                            getConfig().setReadOnly(Boolean.TRUE);
//                            break;
//                        default:
//                            throw new RuntimeException("RO option only valid with SCHEMA_ONLY, LINKED, SQL, and COMMON data strategies.");
//                    }
//                }
//                if (cmd.hasOption("np")) {
//                    getConfig().setNoPurge(Boolean.TRUE);
//                }
//                if (cmd.hasOption("sync") && getConfig().getDataStrategy() != DataStrategyEnum.DUMP) {
//                    getConfig().setSync(Boolean.TRUE);
//                }
//
//                if (cmd.hasOption("dbRegEx")) {
//                    getConfig().getFilter().setDbRegEx(cmd.getOptionValue("dbRegEx"));
//                }
//
//                if (cmd.hasOption("tf")) {
//                    getConfig().getFilter().setTblRegEx(cmd.getOptionValue("tf"));
//                }
//
//                if (cmd.hasOption("tef")) {
//                    getConfig().getFilter().setTblExcludeRegEx(cmd.getOptionValue("tef"));
//                }
//
//                if (cmd.hasOption("tfs")) {
//                    getConfig().getFilter().setTblSizeLimit(Long.parseLong(cmd.getOptionValue("tfs")));
//                }
//
//                if (cmd.hasOption("tfp")) {
//                    getConfig().getFilter().setTblPartitionLimit(Integer.parseInt(cmd.getOptionValue("tfp")));
//                }
//
//            }
//            if (cmd.hasOption("db")) {
//                String[] databases = cmd.getOptionValues("db");
//                if (databases != null)
//                    getConfig().setDatabases(databases);
//            }
//        }
//
////        if (cmd.hasOption("o")) {
////            reportOutputDir = cmd.getOptionValue("o");
////        } else {
////            reportOutputDir = System.getenv("APP_OUTPUT_PATH");
////        }
////
////        if (reportOutputDir == null) {
////            reportOutputDir = System.getProperty("user.home") + System.getProperty("file.separator") + ".hms-mirror" +
////                    System.getProperty("file.separator") + "reports" +
////                    System.getProperty("file.separator") + new SimpleDateFormat("yy-MM-dd_HH-mm-ss").format(new Date());
////        }
//
////        // Action Files
////        reportOutputFile = reportOutputDir + System.getProperty("file.separator") + "<db>_hms-mirror.md|html|yaml";
////        leftExecuteFile = reportOutputDir + System.getProperty("file.separator") + "<db>_LEFT_execute.sql";
////        leftCleanUpFile = reportOutputDir + System.getProperty("file.separator") + "<db>_LEFT_CleanUp_execute.sql";
////        rightExecuteFile = reportOutputDir + System.getProperty("file.separator") + "<db>_RIGHT_execute.sql";
////        rightCleanUpFile = reportOutputDir + System.getProperty("file.separator") + "<db>_RIGHT_CleanUp_execute.sql";
//
////        try {
////            File reportPathDir = new File(reportOutputDir);
////            if (!reportPathDir.exists()) {
////                reportPathDir.mkdirs();
////            }
////        } catch (StringIndexOutOfBoundsException stringIndexOutOfBoundsException) {
////            // no dir in -f variable.
////        }
////        if (reportOutputDir == null) {
////            throw new RuntimeException("Report Output Directory is required. Use -o <dir> to specify or set the APP_OUTPUT_PATH environment variable.");
////        }
//
////        File testFile = new File(reportOutputDir + System.getProperty("file.separator") + ".dir-check");
////
////        // Ensure the Retry Path is created.
////        File retryPath = new File(System.getProperty("user.home") + System.getProperty("file.separator") + ".hms-mirror" +
////                System.getProperty("file.separator") + "retry");
////        if (!retryPath.exists()) {
////            retryPath.mkdirs();
////        }
////
////        // Test file to ensure we can write to it for the report.
////        try {
////            new FileOutputStream(testFile).close();
////        } catch (IOException e) {
////            throw new RuntimeException("Can't write to output directory. ", e);
////        }
//
//        if (!getConfig().isLoadingTestData()) {
//            if (cmd.hasOption("e") && getConfig().getDataStrategy() != DataStrategyEnum.DUMP) {
//                if (cmd.hasOption("accept")) {
//                    getConfig().getAcceptance().setSilentOverride(Boolean.TRUE);
//                } else {
//                    Scanner scanner = new Scanner(System.in);
//
//                    //  prompt for the user's name
//                    System.out.println("----------------------------------------");
//                    System.out.println(".... Accept/Acknowledge to continue ....");
//                    System.out.println("----------------------------------------");
//
//                    if (getConfig().isSync() && !getConfig().isReadOnly()) {
//                        System.out.println("You have chosen to 'sync' WITHOUT 'Read-Only'");
//                        System.out.println("\tWhich means there is a potential for DATA LOSS when out of sync tables are DROPPED and RECREATED.");
//                        System.out.print("\tDo you accept this responsibility/scenario and the potential LOSS of DATA? (YES to proceed)");
//                        String response = scanner.next();
//                        if (!response.equalsIgnoreCase("yes")) {
//                            throw new RuntimeException("You must accept to proceed.");
//                        } else {
//                            getConfig().getAcceptance().setPotentialDataLoss(Boolean.TRUE);
//                        }
//                    }
//
//                    System.out.print("I have made backups of both the 'Hive Metastore' in the LEFT and RIGHT clusters (TRUE to proceed): ");
//                    // get their input as a String
//                    String response = scanner.next();
//                    if (!response.equalsIgnoreCase("true")) {
//                        throw new RuntimeException("You must affirm to proceed.");
//                    } else {
//                        getConfig().getAcceptance().setBackedUpMetastore(Boolean.TRUE);
//                    }
//                    System.out.print("I have taken 'Filesystem' Snapshots/Backups of the target 'Hive Databases' on the LEFT and RIGHT clusters (TRUE to proceed): ");
//                    response = scanner.next();
//                    if (!response.equalsIgnoreCase("true")) {
//                        throw new RuntimeException("You must affirm to proceed.");
//                    } else {
//                        getConfig().getAcceptance().setBackedUpHDFS(Boolean.TRUE);
//                    }
//
//                    System.out.print("'Filesystem' TRASH has been configured on my system (TRUE to proceed): ");
//                    response = scanner.next();
//                    if (!response.equalsIgnoreCase("true")) {
//                        throw new RuntimeException("You must affirm to proceed.");
//                    } else {
//                        getConfig().getAcceptance().setTrashConfigured(Boolean.TRUE);
//                    }
//                }
//                getConfig().setExecute(Boolean.TRUE);
//            } else {
//                log.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
//                log.info("EXECUTE has NOT been set.  No ACTIONS will be performed, the process output will be recorded in the log.");
//                log.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
//                getConfig().setExecute(Boolean.FALSE);
//            }
//        }
//
//        // Set clusters to initialized if we are loading test data.
//        if (getConfig().isLoadingTestData()) {
//            for (Cluster cluster : getConfig().getClusters().values()) {
//                cluster.setInitialized(Boolean.TRUE);
//            }
//        }
//
////        if (!config.validate()) {
////            throw new RuntimeException("Configuration issues., check log (~/.hms-mirror/logs/hms-mirror.log) for details");
////        }
//
//        if (!getConfig().isLoadingTestData()) {
////            ConnectionPools connPools = null;
////            switch (getConfig().getConnectionPoolLib()) {
////                case DBCP2:
////                    log.info("Using DBCP2 Connection Pooling Libraries");
////                    connPools = new ConnectionPoolsDBCP2Impl();
////                    break;
////                case HIKARICP:
////                    log.info("Using HIKARICP Connection Pooling Libraries");
////                    connPools = new ConnectionPoolsHikariImpl();
////                    break;
////                case HYBRID:
////                    log.info("Using HYBRID Connection Pooling Libraries");
////                    connPools = new ConnectionPoolsHybridImpl();
////                    break;
////            }
////            Context.getInstance().setConnectionPools(connPools);
////            Set<Environment> hs2Envs = new HashSet<Environment>();
////            switch (getConfig().getDataStrategy()) {
////                case DUMP:
////                    // Don't load the datasource for the right with DUMP strategy.
////                    if (getConfig().getDumpSource() == Environment.RIGHT) {
////                        // switch LEFT and RIGHT
////                        getConfig().getClusters().remove(Environment.LEFT);
////                        getConfig().getClusters().put(Environment.LEFT, getConfig().getCluster(Environment.RIGHT));
////                        getConfig().getCluster(Environment.LEFT).setEnvironment(Environment.LEFT);
////                        getConfig().getClusters().remove(Environment.RIGHT);
////                    }
////                case STORAGE_MIGRATION:
////                    // Get Pool
////                    connPools.addHiveServer2(Environment.LEFT, getConfig().getCluster(Environment.LEFT).getHiveServer2());
////                    hs2Envs.add(Environment.LEFT);
////                    break;
////                case SQL:
////                case SCHEMA_ONLY:
////                case EXPORT_IMPORT:
////                case HYBRID:
////                    // When doing inplace downgrade of ACID tables, we're only dealing with the LEFT cluster.
////                    if (!getConfig().getMigrateACID().isInplace() && null != getConfig().getCluster(Environment.RIGHT).getHiveServer2()) {
////                        connPools.addHiveServer2(Environment.RIGHT, getConfig().getCluster(Environment.RIGHT).getHiveServer2());
////                        hs2Envs.add(Environment.RIGHT);
////                    }
////                default:
////                    connPools.addHiveServer2(Environment.LEFT, getConfig().getCluster(Environment.LEFT).getHiveServer2());
////                    hs2Envs.add(Environment.LEFT);
////                    break;
////            }
////            if (Context.getInstance().loadPartitionMetadata()) {
////                if (getConfig().getCluster(Environment.LEFT).getMetastoreDirect() != null) {
////                    connPools.addMetastoreDirect(Environment.LEFT, getConfig().getCluster(Environment.LEFT).getMetastoreDirect());
////                }
////                if (getConfig().getCluster(Environment.RIGHT).getMetastoreDirect() != null) {
////                    connPools.addMetastoreDirect(Environment.RIGHT, getConfig().getCluster(Environment.RIGHT).getMetastoreDirect());
////                }
////            }
////            try {
////                connPools.init();
////                for (Environment target : hs2Envs) {
////                    Connection conn = null;
////                    Statement stmt = null;
////                    try {
////                        conn = connPools.getHS2EnvironmentConnection(target);
////                        if (conn == null) {
////                            if (target == Environment.RIGHT && getConfig().getCluster(target).getHiveServer2().isDisconnected()) {
////                                // Skip error.  Set Warning that we're disconnected.
////                                getConfig().getWarnings().set(ENVIRONMENT_DISCONNECTED.getCode(), new Object[]{target});
////                            } else {
////                                getConfig().getErrors().set(ENVIRONMENT_CONNECTION_ISSUE.getCode(), new Object[]{target});
////                                return getConfig().getErrors().getReturnCode();
////                            }
////                        } else {
////                            // Exercise the connection.
////                            stmt = conn.createStatement();
////                            stmt.execute("SELECT 1");
////                        }
////                    } catch (SQLException se) {
////                        if (target == Environment.RIGHT && getConfig().getCluster(target).getHiveServer2().isDisconnected()) {
////                            // Set warning that RIGHT is disconnected.
////                            getConfig().getWarnings().set(ENVIRONMENT_DISCONNECTED.getCode(), new Object[]{target});
////                        } else {
////                            log.error(se.getMessage(), se);
////                            getConfig().getErrors().set(ENVIRONMENT_CONNECTION_ISSUE.getCode(), new Object[]{target});
////                            return getConfig().getErrors().getReturnCode();
////                        }
////                    } catch (Throwable t) {
////                        log.error(t.getMessage(), t);
////                        getConfig().getErrors().set(ENVIRONMENT_CONNECTION_ISSUE.getCode(), new Object[]{target});
////                        return getConfig().getErrors().getReturnCode();
////                    } finally {
////                        if (stmt != null) {
////                            stmt.close();
////                        }
////                        if (conn != null) {
////                            conn.close();
////                        }
////                    }
////                }
////            } catch (SQLException cnfe) {
////                log.error("Issue initializing connections.  Check driver locations", cnfe);
////                return -1;
//////                throw new RuntimeException(cnfe);
////            }
////
////            getConfig().getCluster(Environment.LEFT).setPools(connPools);
////            switch (getConfig().getDataStrategy()) {
////                case DUMP:
////                    // Don't load the datasource for the right with DUMP strategy.
////                    break;
////                default:
////                    // Don't set the Pools when Disconnected.
////                    if (getConfig().getCluster(Environment.RIGHT).getHiveServer2() != null && !getConfig().getCluster(Environment.RIGHT).getHiveServer2().isDisconnected()) {
////                        getConfig().getCluster(Environment.RIGHT).setPools(connPools);
////                    }
////            }
////
////            if (getConfig().isConnectionKerberized()) {
////                log.debug("Detected a Kerberized JDBC Connection.  Attempting to setup/initialize GSS.");
////                setupGSS();
////            }
////            log.debug("Checking Hive Connections");
////            if (!getConfig().checkConnections()) {
////                log.error("Check Hive Connections Failed.");
////                if (getConfig().isConnectionKerberized()) {
////                    log.error("Check Kerberos configuration if GSS issues are encountered.  See the running.md docs for details.");
////                }
////                throw new RuntimeException("Check Hive Connections Failed.  Check Logs.");
////            }
//        }
//        return rtn;
//    }

//    public Config loadConfig(CommandLine cmd) {
//        Config config = null;
//        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
//        mapper.enable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
//
//        // Initialize with config and output directory.
//        if (cmd.hasOption("cfg")) {
//            configFile = cmd.getOptionValue("cfg");
//        } else {
//            configFile = System.getProperty("user.home") + System.getProperty("file.separator") + ".hms-mirror/cfg/default.yaml";
//            File defaultCfg = new File(configFile);
//            if (!defaultCfg.exists()) {
//                Config.setup(configFile);
//                System.exit(0);
//            }
//        }
//
//        URL cfgUrl = null;
//
//        File cfgFile = new File(configFile);
//        if (!cfgFile.exists()) {
//            // Try loading from resource (classpath).  Mostly for testing.
//            cfgUrl = this.getClass().getResource(configFile);
//            if (cfgUrl == null) {
//                throw new RuntimeException("Couldn't locate configuration file: " + configFile);
//            }
//            log.info("Using 'classpath' config: " + configFile);
//        } else {
//            log.info("Using filesystem config: " + configFile);
//            try {
//                cfgUrl = cfgFile.toURI().toURL();
//            } catch (MalformedURLException mfu) {
//                throw new RuntimeException("Couldn't locate configuration file: " + configFile, mfu);
//            }
//        }
//
//        log.info("Check log '" + System.getProperty("app.path.dir") + System.getProperty("file.separator") + System.getProperty("app.log.file") +
//                " for progress.");
//
//        try {
//            String yamlCfgFile = IOUtils.toString(cfgUrl, StandardCharsets.UTF_8);
//            config = mapper.readerFor(Config.class).readValue(yamlCfgFile);
//            Context.getInstance().setConfig(config);
//        } catch (UnrecognizedPropertyException upe) {
//            System.out.println("\n>>>>>   READ THIS BEFORE CONTINUING.  Minor configuration fix REQUIRED.  <<<<<");
//            throw new RuntimeException("\nThere may have been a breaking change in the configuration since the previous " +
//                    "release. Review the note below and remove the 'Unrecognized field' from the configuration and try " +
//                    "again.\n\n", upe);
//        } catch (Throwable t) {
//            // Look for yaml update errors.
//            if (t.toString().contains("MismatchedInputException")) {
//                throw new RuntimeException("The format of the 'config' yaml file MAY HAVE CHANGED from the last release.  Please make a copy and run " +
//                        "'-su|--setup' again to recreate in the new format", t);
//            } else {
//                log.error(t.getMessage(), t);
//                throw new RuntimeException("A configuration element is no longer valid, progress.  Please remove the element from the configuration yaml and try again.", t);
//            }
//        }
//
//        if (cmd.hasOption("p") || cmd.hasOption("dp")) {
//            // Used to generate encrypted password.
//            if (cmd.hasOption("pkey")) {
//                Protect protect = new Protect(cmd.getOptionValue("pkey"));
//                // Set to control execution flow.
//                config.getErrors().set(MessageCode.PASSWORD_CFG.getCode());
//                if (cmd.hasOption("p")) {
//                    String epassword = null;
//                    try {
//                        epassword = protect.encrypt(cmd.getOptionValue("p"));
//                        config.getWarnings().set(MessageCode.ENCRYPTED_PASSWORD.getCode(), epassword);
//                    } catch (Exception e) {
//                        config.getErrors().set(MessageCode.ENCRYPT_PASSWORD_ISSUE.getCode());
//                    }
//                } else {
//                    String password = null;
//                    try {
//                        password = protect.decrypt(cmd.getOptionValue("dp"));
//                        config.getWarnings().set(MessageCode.DECRYPTED_PASSWORD.getCode(), password);
//                    } catch (Exception e) {
//                        config.getErrors().set(MessageCode.DECRYPTING_PASSWORD_ISSUE.getCode());
//                    }
//                }
//            } else {
//                config.getErrors().set(MessageCode.PKEY_PASSWORD_CFG.getCode());
//            }
//        }
//        return config;
//    }

    /*
    The directory is the output from the original run of the process.

    NOTE: The report directory may have more than one set of files (one for each database).


    1. Find the *_hms-mirror.yaml file and load it.
        - If more than one db, loop through them all and run them.
    2. Find the *_hms-mirror.md file and extract the 'config' from it.
        - Load the config.
    3. Remove the -execute, if set.
    4. Set the loadTestData Flag.
    5. run process.
     */
//    private void replay(String reportDirectory) {
//        // 1. Find the *_hms-mirror.yaml file and load it.
//        File[] files = new File(reportDirectory).listFiles(new FilenameFilter() {
//            public boolean accept(File dir, String name) {
//                return name.toLowerCase().endsWith("_hms-mirror.yaml");
//            }
//        });
//        if (files.length == 0) {
//            log.error("No report files found in: " + reportDirectory);
//            return;
//        }
//        conversion = new Conversion();
//        for (File reportFile : files) {
//            log.info("Found report file: " + reportFile.getAbsolutePath());
//            DBMirror dbMirror = DBMirror.load(reportFile.getAbsolutePath());
//            // Set the table's phase state to INIT
//            for (TableMirror tableMirror : dbMirror.getTableMirrors().values()) {
//                tableMirror.setPhaseState(PhaseState.INIT);
//                for (EnvironmentTable environmentTable : tableMirror.getEnvironments().values()) {
//                    environmentTable.getIssues().clear();
//                    environmentTable.getSql().clear();
//                    environmentTable.getCleanUpSql().clear();
//                }
//            }
//            conversion.addDBMirror(dbMirror);
//        }
//
//
//        // 2. Find the *_hms-mirror.md file and extract the 'config' from it.
//        files = new File(reportDirectory).listFiles(new FilenameFilter() {
//            public boolean accept(File dir, String name) {
//                return name.toLowerCase().endsWith("_hms-mirror.md");
//            }
//        });
//        if (files.length == 0) {
//            log.error("No report files found in: " + reportDirectory);
//            return;
//        }
//        File configFile = files[0];
//        log.info("Found config file: " + configFile.getAbsolutePath());
//        // Review the config file line by line
//        try {
//            BufferedReader br = new BufferedReader(new FileReader(configFile));
//            String line;
//            while ((line = br.readLine()) != null) {
//                if (line.startsWith("```")) {
//                    // Start of the config
//                    StringBuilder sb = new StringBuilder();
//                    while ((line = br.readLine()) != null) {
//                        if (line.startsWith("```")) {
//                            // End of the config
//                            break;
//                        }
//                        // Don't load the this line.
//                        if (!line.startsWith("loadingTestData"))
//                            sb.append(line).append("\n");
//                    }
//                    // Load the config
//                    // TODO: Need to handle config load from Replay File.
////                    config = Config.load(sb.toString());
//                    config.setExecute(Boolean.FALSE);
//                    config.setLoadTestDataFile("replay-" + reportDirectory);
//                    config.setReplay(Boolean.TRUE);
//                    Context.getInstance().setConfig(config);
//                    break;
//                }
//            }
//            br.close();
//        } catch (IOException ioe) {
//            log.error("Issue reading config file: " + configFile.getAbsolutePath(), ioe);
//            return;
//        }
//    }

    // Converted to Application.collect and TransferService.transfer.
//    public Conversion runTransfer(Conversion conversion) {
//        Date startTime = new Date();
//        log.info("Start Processing for databases: " + Arrays.toString((getConfig().getDatabases())));
//
//        log.info(">>>>>>>>>>> Building/Starting Transition.");
//        List<Future<ReturnStatus>> mdf = new ArrayList<Future<ReturnStatus>>();
//
//        // Loop through databases
//        Set<String> collectedDbs = conversion.getDatabases().keySet();
//        for (String database : collectedDbs) {
//            DBMirror dbMirror = conversion.getDatabase(database);
//            if (config.getDataStrategy() == DataStrategyEnum.ICEBERG_CONVERSION) {
//                dbMirror.addIssue(Environment.LEFT, "This will only process tables that are not already Iceberg.");
//                dbMirror.addIssue(Environment.LEFT, "This Hive Script will only run against HS2's on CDP PvC DS 1.5.1+, CDP Public Cloud Datahub August 2023+, or CDP Data Warehouse Public Cloud August 2023+");
//                dbMirror.addIssue(Environment.LEFT, "CDP Private Cloud Base 7.1.9 does NOT support Hive with Iceberg.  This will fail.");
//            }
//            // Loop through the tables in the database
//            Set<String> tables = dbMirror.getTableMirrors().keySet();
//            for (String table : tables) {
//                TableMirror tblMirror = dbMirror.getTableMirrors().get(table);
//                switch (tblMirror.getPhaseState()) {
//                    case INIT:
//                    case STARTED:
//                    case ERROR:
//                        // Create a Transfer for the table.
//                        TransferService md = new TransferService(config, dbMirror, tblMirror);
//                        mdf.add(getConfig().getTransferThreadPool().schedule(md, 1, TimeUnit.MILLISECONDS));
//                        break;
//                    case SUCCESS:
//                        log.debug("DB.tbl: " + tblMirror.getParent().getName() + "." + tblMirror.getName(Environment.LEFT) + " was SUCCESSFUL in " +
//                                "previous run.   SKIPPING and adjusting status to RETRY_SKIPPED_PAST_SUCCESS");
//                        tblMirror.setPhaseState(PhaseState.RETRY_SKIPPED_PAST_SUCCESS);
//                        break;
//                    case RETRY_SKIPPED_PAST_SUCCESS:
//                        log.debug("DB.tbl: " + tblMirror.getParent().getName() + "." + tblMirror.getName(Environment.LEFT) + " was SUCCESSFUL in " +
//                                "previous run.  SKIPPING");
//                }
//            }
//        }
//
//        log.info(">>>>>>>>>>> Starting Transfer.");
//
//        while (true) {
//            boolean check = true;
//            for (Future<ReturnStatus> sf : mdf) {
//                if (!sf.isDone()) {
//                    check = false;
//                    break;
//                }
//                try {
//                    if (sf.isDone() && sf.get() != null) {
//                        switch (sf.get().getStatus()) {
//                            case SUCCESS:
//                                break;
//                            case ERROR:
//                            case FATAL:
//                                throw new RuntimeException(sf.get().getException());
//                        }
//                    }
//                } catch (InterruptedException | ExecutionException e) {
//                    throw new RuntimeException(e);
//                }
//            }
//            if (check)
//                break;
//        }
//
//        getConfig().getTransferThreadPool().shutdown();
//
//        log.info("==============================");
//        log.info(conversion.toString());
//        log.info("==============================");
//        Date endTime = new Date();
//        DecimalFormat df = new DecimalFormat("#.###");
//        df.setRoundingMode(RoundingMode.CEILING);
//        log.info("METADATA-STAGE: Completed in " + df.format((Double) ((endTime.getTime() - startTime.getTime()) / (double) 1000)) + " secs");
//
//        return conversion;
//    }

//    protected void setupGSS() {
//        try {
//            String CURRENT_USER_PROP = "current.user";
//
//            String HADOOP_CONF_DIR = "HADOOP_CONF_DIR";
//            String[] HADOOP_CONF_FILES = {"core-site.xml", "hdfs-site.xml", "mapred-site.xml", "yarn-site.xml"};
//
//            // Get a value that over rides the default, if nothing then use default.
//            String hadoopConfDirProp = System.getenv().getOrDefault(HADOOP_CONF_DIR, "/etc/hadoop/conf");
//
//            // Set a default
//            if (hadoopConfDirProp == null)
//                hadoopConfDirProp = "/etc/hadoop/conf";
//
//            Configuration hadoopConfig = new Configuration(true);
//
//            File hadoopConfDir = new File(hadoopConfDirProp).getAbsoluteFile();
//            for (String file : HADOOP_CONF_FILES) {
//                File f = new File(hadoopConfDir, file);
//                if (f.exists()) {
//                    log.debug("Adding conf resource: '" + f.getAbsolutePath() + "'");
//                    try {
//                        // I found this new Path call failed on the Squadron Clusters.
//                        // Not sure why.  Anyhow, the above seems to work the same.
//                        hadoopConfig.addResource(new Path(f.getAbsolutePath()));
//                    } catch (Throwable t) {
//                        // This worked for the Squadron Cluster.
//                        // I think it has something to do with the Docker images.
//                        hadoopConfig.addResource("file:" + f.getAbsolutePath());
//                    }
//                }
//            }
//
//            // hadoop.security.authentication
//            if (hadoopConfig.get("hadoop.security.authentication", "simple").equalsIgnoreCase("kerberos")) {
//                try {
//                    UserGroupInformation.setConfiguration(hadoopConfig);
//                } catch (Throwable t) {
//                    // Revert to non JNI. This happens in Squadron (Docker Imaged Hosts)
//                    log.error("Failed GSS Init.  Attempting different Group Mapping");
//                    hadoopConfig.set("hadoop.security.group.mapping", "org.apache.hadoop.security.ShellBasedUnixGroupsMapping");
//                    UserGroupInformation.setConfiguration(hadoopConfig);
//                }
//            }
//        } catch (Throwable t) {
//            log.error("Issue initializing Kerberos", t);
//            t.printStackTrace();
//            throw t;
//        }
//    }

//    public Boolean setupSql(Environment environment, List<Pair> sqlPairList) {
//        Boolean rtn = Boolean.TRUE;
//        rtn = getConfig().getCluster(environment).runClusterSql(sqlPairList);
//        return rtn;
//    }

//    public long setupSql(String[] args, List<Pair> leftSql, List<Pair> rightSql) {
//        long returnCode = 0;
//        log.info("===================================================");
//        log.info("Running: hms-mirror " + ReportingConf.substituteVariablesFromManifest("v.${HMS-Mirror-Version}"));
//        log.info(" with commandline parameters: " + String.join(",", args));
//        log.info("===================================================");
//        log.info("");
//        log.info("======  SQL Setup ======");
//        try {
//            returnCode = init(args);
//            try {
//                if (leftSql != null && leftSql.size() > 0) {
//                    if (!setupSql(Environment.LEFT, leftSql)) {
//                        log.error("Failed to run LEFT SQL, check Logs");
//                        returnCode = -1;
//                    }
//                }
//                if (rightSql != null && rightSql.size() > 0) {
//                    if (!setupSql(Environment.RIGHT, rightSql)) {
//                        log.error("Failed to run RIGHT SQL, check Logs");
//                        returnCode = -1;
//                    }
//                }
//            } catch (RuntimeException rte) {
//                System.out.println(rte.getMessage());
//                rte.printStackTrace();
//                if (config != null) {
//                    returnCode = getConfig().getErrors().getReturnCode(); //MessageCode.returnCode(getConfig().getErrors());
//                } else {
//                    returnCode = -1;
//                }
//            }
//        } catch (RuntimeException e) {
//            log.error(e.getMessage(), e);
//            System.err.println("=====================================================");
//            System.err.println("Commandline args: " + Arrays.toString(args));
//            System.err.println();
//            log.error("Commandline args: " + Arrays.toString(args));
//            if (config != null) {
//                for (String error : getConfig().getErrors().getMessages()) {
//                    log.error(error);
//                    System.err.println(error);
//                }
//                returnCode = getConfig().getErrors().getReturnCode();
//            } else {
//                returnCode = -1;
//            }
//            System.err.println(e.getMessage());
//            e.printStackTrace();
//            System.err.println("\nSee log for stack trace ($HOME/.hms-mirror/logs)");
//        }
//        return returnCode;
//    }

//    public long setupSqlLeft(String[] args, List<Pair> sqlPairList) {
//        long rtn = 0l;
//        rtn = setupSql(args, sqlPairList, null);
//        return rtn;
//    }

//    public long setupSqlRight(String[] args, List<Pair> sqlPairList) {
//        long rtn = 0l;
//        rtn = setupSql(args, null, sqlPairList);
//        return rtn;
//    }
}
