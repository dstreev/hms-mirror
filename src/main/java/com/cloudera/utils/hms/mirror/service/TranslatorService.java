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

import com.cloudera.utils.hms.mirror.EnvironmentMap;
import com.cloudera.utils.hms.mirror.EnvironmentTable;
import com.cloudera.utils.hms.mirror.PhaseState;
import com.cloudera.utils.hms.mirror.domain.*;
import com.cloudera.utils.hms.mirror.domain.support.*;
import com.cloudera.utils.hms.mirror.exceptions.MismatchException;
import com.cloudera.utils.hms.mirror.exceptions.MissingDataPointException;
import com.cloudera.utils.hms.mirror.exceptions.RequiredConfigurationException;
import com.cloudera.utils.hms.mirror.exceptions.SessionException;
import com.cloudera.utils.hms.util.NamespaceUtils;
import com.cloudera.utils.hms.util.TableUtils;
import com.cloudera.utils.hms.util.UrlUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.text.MessageFormat;
import java.util.*;

import static com.cloudera.utils.hms.mirror.MessageCode.LOCATION_NOT_MATCH_WAREHOUSE;
import static com.cloudera.utils.hms.mirror.MessageCode.RDL_W_EPL_NO_MAPPING;
import static com.cloudera.utils.hms.mirror.MirrorConf.*;
import static java.util.Objects.isNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

@Service
@Slf4j
public class TranslatorService {

    @Getter
    private ExecuteSessionService executeSessionService = null;

//    private ConfigService configService;
    private DatabaseService databaseService;

//    @Autowired
//    public void setConfigService(ConfigService configService) {
//        this.configService = configService;
//    }

    /**
     * @param consolidationLevel how far up the directory hierarchy to go to build the distcp list based on the sources
     *                           provided.
     * @return A map of databases.  Each database will have a map that has 1 or more 'targets' and 'x' sources for each
     * target.
     */
    public synchronized Map<String, Map<String, Set<String>>> buildDistcpList(String database, Environment environment, int consolidationLevel) {
        Map<String, Map<String, Set<String>>> rtn = new TreeMap<>();

        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();

        // get the map for a db.
        Set<String> databases = hmsMirrorConfig.getTranslator().getTranslationMap().keySet();

        // get the map.entry
        Map<String, Set<String>> reverseMap = new TreeMap<>();
        // Get a static view of set to avoid concurrent modification.
        Set<EnvironmentMap.TranslationLevel> dbTranslationLevel =
                new HashSet<>(hmsMirrorConfig.getTranslator().getTranslationMap(database, environment));

        Map<String, String> dbLocationMap = new TreeMap<>();

        for (EnvironmentMap.TranslationLevel translationLevel : dbTranslationLevel) {
            if (translationLevel.getOriginal() != null &&
                translationLevel.getTarget() != null) {
                dbLocationMap.put(translationLevel.getAdjustedOriginal(), translationLevel.getAdjustedTarget());
            }
        }

        for (Map.Entry<String, String> entry : dbLocationMap.entrySet()) {
            // reduce folder level by 'consolidationLevel' for key and value.
            // Source
            String reducedSource = UrlUtils.reduceUrlBy(entry.getKey(), consolidationLevel);
            // Target
            String reducedTarget = UrlUtils.reduceUrlBy(entry.getValue(), consolidationLevel);

            if (reverseMap.get(reducedTarget) != null) {
                reverseMap.get(reducedTarget).add(entry.getKey());
            } else {
                Set<String> sourceSet = new TreeSet<String>();
                sourceSet.add(entry.getKey());
                reverseMap.put(reducedTarget, sourceSet);
            }

        }
        rtn.put(database, reverseMap);
        return rtn;
    }

    public String buildPartitionAddStatement(EnvironmentTable environmentTable) {
        StringBuilder sbPartitionDetails = new StringBuilder();
        Map<String, String> partitions = new HashMap<String, String>();
        // Fix formatting of partition names.
        for (Map.Entry<String, String> item : environmentTable.getPartitions().entrySet()) {
            String partitionName = item.getKey();
            String partSpec = TableUtils.toPartitionSpec(partitionName);
            partitions.put(partSpec, item.getValue());
        }
        // Transfer partitions map to a string using streaming
        partitions.entrySet().stream().forEach(e -> sbPartitionDetails.append("\tPARTITION (")
                .append(e.getKey()).append(") LOCATION '").append(e.getValue()).append("' \n"));
        return sbPartitionDetails.toString();
    }

    public String processGlobalLocationMap(String originalLocation) {
        String newLocation = null;
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();

        if (!hmsMirrorConfig.getTranslator().getOrderedGlobalLocationMap().isEmpty()) {
            log.debug("Checking location: {} for replacement element in global location map.", originalLocation);
            for (String key : hmsMirrorConfig.getTranslator().getOrderedGlobalLocationMap().keySet()) {
                if (originalLocation.startsWith(key)) {
                    String rLoc = hmsMirrorConfig.getTranslator().getOrderedGlobalLocationMap().get(key);
                    newLocation = originalLocation.replace(key, rLoc);
                    log.info("Location Map Found. {}:{} New Location: {}", key, rLoc, newLocation);
                    // Stop Processing
                    break;
                }
            }
        }
        if (newLocation != null)
            return newLocation;
        else
            return originalLocation;
    }

    @Autowired
    public void setDatabaseService(DatabaseService databaseService) {
        this.databaseService = databaseService;
    }

    @Autowired
    public void setExecuteSessionService(ExecuteSessionService executeSessionService) {
        this.executeSessionService = executeSessionService;
    }

//    public Warehouse getDatabaseWarehouse(String database) throws MissingDataPointException {
//        Warehouse dbWarehouse = null;
//        ExecuteSession session = executeSessionService.getSession();
//        HmsMirrorConfig config = session.getConfig();
//        dbWarehouse = config.getTranslator().getWarehouseMapBuilder().getWarehousePlans().get(database);
//        if (isNull(dbWarehouse)) {
//            if (config.getTransfer().getWarehouse().getManagedDirectory() != null &&
//                    config.getTransfer().getWarehouse().getExternalDirectory() != null) {
//                dbWarehouse = new Warehouse(config.getTransfer().getWarehouse().getExternalDirectory(),
//                        config.getTransfer().getWarehouse().getManagedDirectory());
//            }
//        }
//        if (isNull(dbWarehouse)) {
//            // Look for Location in the right DB Definition for Migration Strategies.
//            switch (config.getDataStrategy()) {
//                case SCHEMA_ONLY:
//                case EXPORT_IMPORT:
//                case HYBRID:
//                case SQL:
//                case COMMON:
//                case LINKED:
//                    if (nonNull(config.getCluster(Environment.RIGHT).getEnvVars())) {
//                        String extDir = config.getCluster(Environment.RIGHT).getEnvVars().get(EXT_DB_LOCATION_PROP);
//                        String manDir = config.getCluster(Environment.RIGHT).getEnvVars().get(MNGD_DB_LOCATION_PROP);
//                        if (extDir != null && manDir != null) {
//                            dbWarehouse = new Warehouse(extDir, manDir);
//                            session.addWarning(WAREHOUSE_DIRECTORIES_RETRIEVED_FROM_HIVE_ENV);
//                        } else {
//                            session.addError(WAREHOUSE_DIRECTORIES_NOT_DEFINED);
//                            throw new MissingDataPointException("Couldn't find a Warehouse Plan for database: " + database +
//                                    ". The global warehouse locations aren't defined either.  Please define a warehouse plan or " +
//                                    "set the global warehouse locations.");
//                        }
//                    } else {
//                        session.addError(WAREHOUSE_DIRECTORIES_NOT_DEFINED);
//                        throw new MissingDataPointException("Couldn't find a Warehouse Plan for database: " + database +
//                                ". The global warehouse locations aren't defined either.  Please define a warehouse plan or " +
//                                "set the global warehouse locations.");
//                    }
//                    break;
//                default: // STORAGE_MIGRATION should set these manually.
//                    session.addError(WAREHOUSE_DIRECTORIES_NOT_DEFINED);
//                    throw new MissingDataPointException("Couldn't find a Warehouse Plan for database: " + database +
//                            ". The global warehouse locations aren't defined either.  Please define a warehouse plan or " +
//                            "set the global warehouse locations.");
//            }
//        }
//        return dbWarehouse;
//    }

    public Boolean translatePartitionLocations(TableMirror tblMirror) {
        Boolean rtn = Boolean.TRUE;
        HmsMirrorConfig config = executeSessionService.getSession().getConfig();

//        Warehouse warehouse = databaseService.getWarehousePlan(tblMirror.getParent().getName());

//        Map<String, String> dbRef = tblMirror.getParent().getDBDefinition(Environment.RIGHT);
//        Boolean chkLocation = config.getTransfer().getWarehouse().getManagedDirectory() != null && config.getTransfer().getWarehouse().getExternalDirectory() != null;
        if (config.isEvaluatePartitionLocation()
                && tblMirror.getEnvironmentTable(Environment.LEFT).getPartitioned()
                && (tblMirror.getStrategy() == DataStrategyEnum.SCHEMA_ONLY)) {
            // Only Translate for SCHEMA_ONLY.  Leave the DUMP location as is.
            EnvironmentTable target = tblMirror.getEnvironmentTable(Environment.RIGHT);
            /*
            Review the target partition locations and replace the namespace with the new namespace.
            Check whether any global location maps match the location and adjust.
             */
            Map<String, String> partitionLocationMap = target.getPartitions();
            if (partitionLocationMap != null && !partitionLocationMap.isEmpty()) {
                for (Map.Entry<String, String> entry : partitionLocationMap.entrySet()) {
                    String partitionLocation = entry.getValue();
                    String partSpec = entry.getKey();
                    int level = StringUtils.countMatches(partSpec, "/");
                    // Increase level to the table, since we're not filter any tables.  It's assumed that
                    //   we're pulling the whole DB.
                    if (!config.getFilter().isTableFiltering()) {
                        level++;
                    }
                    if (isBlank(partitionLocation) || partitionLocation.isEmpty() ||
                            partitionLocation.equals(NOT_SET)) {
                        rtn = Boolean.FALSE;
                        continue;
                    }
                    // Get the relative dir.
                    String relativeDir = partitionLocation.replace(config.getCluster(Environment.LEFT).getHcfsNamespace(), "");
                    // Check the Global Location Map for a match.
                    String mappedDir = processGlobalLocationMap(relativeDir);
                    if (relativeDir.equals(mappedDir) && config.isResetToDefaultLocation()) {
                        // This is a problem, since we've asked to translate the partitions but didn't find a map, nothing changed.
                        // Which would be inconsistent with the table location details.
                        String errMsg = MessageFormat.format(RDL_W_EPL_NO_MAPPING.getDesc(), entry.getKey(), entry.getValue());
                        tblMirror.addIssue(Environment.RIGHT, errMsg);
                        rtn = Boolean.FALSE;
                    }
                    // Check for 'common storage'
                    String newPartitionLocation = null;
                    if (config.getTransfer().getCommonStorage() != null) {
                        newPartitionLocation = config.getTransfer().getCommonStorage() + mappedDir;
                    } else {
                        newPartitionLocation = config.getCluster(Environment.RIGHT).getHcfsNamespace() + mappedDir;
                    }
                    entry.setValue(newPartitionLocation);
                    // For distcp.
                    config.getTranslator().addTranslation(HmsMirrorConfigUtil.getResolvedDB(tblMirror.getParent().getName(), config), Environment.RIGHT, partitionLocation,
                            newPartitionLocation, ++level);

                    // Check and warn against warehouse locations if specified.
//                    if (config.getTransfer().getWarehouse().getExternalDirectory() != null &&
//                            config.getTransfer().getWarehouse().getManagedDirectory() != null) {
                        if (TableUtils.isExternal(tblMirror.getEnvironmentTable(Environment.LEFT))) {
                            // We store the DB LOCATION in the RIGHT dbDef so we can avoid changing the original LEFT
                            if (!newPartitionLocation.startsWith(tblMirror.getParent().getDBDefinition(Environment.RIGHT).get(DB_LOCATION))) {
                                // Set warning that even though you've specified to warehouse directories, the current configuration
                                // will NOT place it in that directory.
                                String msg = MessageFormat.format(LOCATION_NOT_MATCH_WAREHOUSE.getDesc(), "partition",
                                        tblMirror.getParent().getDBDefinition(Environment.RIGHT).get(DB_LOCATION),
                                        newPartitionLocation);
                                tblMirror.addIssue(Environment.RIGHT, msg);
                            }
                        } else {
                            if (!newPartitionLocation.startsWith(tblMirror.getParent().getDBDefinition(Environment.RIGHT).get(DB_MANAGED_LOCATION))) {
                                // Set warning that even though you've specified to warehouse directories, the current configuration
                                // will NOT place it in that directory.
                                String msg = MessageFormat.format(LOCATION_NOT_MATCH_WAREHOUSE.getDesc(), "partition",
                                        tblMirror.getParent().getDBDefinition(Environment.RIGHT).get(DB_MANAGED_LOCATION),
                                        newPartitionLocation);
                                tblMirror.addIssue(Environment.RIGHT, msg);
                            }
                        }
//                    }

                }
            }
            // end partitions location conversion.
        }
        return rtn;
    }

    public String translateTableLocation(TableMirror tableMirror, String originalLocation,
                                         int level, String partitionSpec)
            throws MismatchException, MissingDataPointException, RequiredConfigurationException {
        String rtn = originalLocation;
        StringBuilder dirBuilder = new StringBuilder();
        String tableName = tableMirror.getName();
        HmsMirrorConfig config = executeSessionService.getSession().getConfig();

        String dbName = HmsMirrorConfigUtil.getResolvedDB(tableMirror.getParent().getName(), config);

//        String leftNS = config.getCluster(Environment.LEFT).getHcfsNamespace();
        // Set base on rightNS or Common Storage, if specified
        String rightNS = config.getTargetNamespace();
//                isBlank(config.getTransfer().getCommonStorage()) ?
//                config.getCluster(Environment.RIGHT).getHcfsNamespace() : config.getTransfer().getCommonStorage();

        // Get the relative dir.
        // MissingDataPointException below is throw when we can't find a warehouse plan for the table
//        if (!rtn.startsWith(config.getCluster(Environment.LEFT).getHcfsNamespace())) {
//            throw new MismatchException("Table/Partition Location prefix: `" + originalLocation +
//                    "` doesn't match the LEFT clusters defined hcfsNamespace: `" + config.getCluster(Environment.LEFT).getHcfsNamespace() +
//                    "`. We can't reliably make this translation.");
//        }

//        String relativeDir = rtn.replace(config.getCluster(Environment.LEFT).getHcfsNamespace(), "");
        String relativeDir = NamespaceUtils.stripNamespace(rtn);

        // Check the Global Location Map for a match.
        String mappedDir = processGlobalLocationMap(relativeDir);
        // If they don't match, it was reMapped!
        boolean reMapped = !relativeDir.equals(mappedDir);
        if (reMapped) {
            tableMirror.setReMapped(Boolean.TRUE);
        } else {
            // under conditions like, STORAGE_MIGRATION, same namespace, !rdl and glm we need to ensure ALL locations are
            //   mapped...  If they aren't, they won't be moved as the translation wouldn't change.  So we need to throw
            //   an error that ensures the table fails to process.

            // Get the Namespace from the original table location.
            String origNamespace = NamespaceUtils.getNamespace(originalLocation);

            if (config.getDataStrategy() == DataStrategyEnum.STORAGE_MIGRATION
                    && origNamespace.equals(config.getTransfer().getCommonStorage()) //.equals(config.getCluster(Environment.LEFT).getHcfsNamespace())
                    && !config.isResetToDefaultLocation()) {
//                tableMirror.addIssue(Environment.LEFT, "Location Mapping can't be determined.  No matching `glm` entry to make translation." +
//                        "Original Location: " + originalLocation);
                tableMirror.setPhaseState(PhaseState.ERROR);
                throw new MissingDataPointException("Location Mapping can't be determined.  No matching `glm` entry to make translation." +
                        "Original Location: " + originalLocation);
            }
        }
        // Feature Off.  Basic translation which includes any GlobalLocationMaps.
        String newLocation = null;
        StringBuilder sbDir = new StringBuilder();
//        if (config.getTransfer().getCommonStorage() != null) {
//            sbDir.append(config.getTransfer().getCommonStorage());
//        } else {
            sbDir.append(rightNS);
//        }
        if (reMapped) {
            sbDir.append(mappedDir);
            newLocation = sbDir.toString();
        } else if (config.isResetToDefaultLocation()) {
            // RDL
            Warehouse warehouse = databaseService.getWarehousePlan(dbName);
            // This shouldn't be null. Should've been caught earlier.
            // TODO: This can be null so we need to throw an error when it is.
            //       This means that a warehouse wasn't configured for the database (globally or etc.)
            //  MAYBE raise MissingDataPointException here.  Need to follow upward path to see if it's handled.
//            if (isNull(warehouse)) {
//                throw new MissingDataPointException("GLM not found for relative directory of table and couldn't find a Warehouse Plan for database: " + dbName +
//                        "table: " + tableName + ". The global warehouse locations aren't defined either.  Please define a warehouse plan or " +
//                        "set the global warehouse locations.");
//            }
//            assert warehouse != null;
            // Using the RIGHT to determine the table type.  This will account for a table
            //   definition that's been converted already and ensure the correct location is used.
            EnvironmentTable checkEnvTbl = tableMirror.getEnvironmentTable(Environment.RIGHT);
            if (isNull(checkEnvTbl) || checkEnvTbl.getDefinition().isEmpty()) {
                checkEnvTbl = tableMirror.getEnvironmentTable(Environment.LEFT);
            }
            if (TableUtils.isManaged(checkEnvTbl)) {
                sbDir.append(warehouse.getManagedDirectory()).append("/");
                sbDir.append(dbName).append(".db").append("/").append(tableName);
                if (partitionSpec != null)
                    sbDir.append("/").append(partitionSpec);
                newLocation = sbDir.toString();
            } else if (TableUtils.isExternal(checkEnvTbl)) {
                sbDir.append(warehouse.getExternalDirectory()).append("/");
                sbDir.append(dbName).append(".db").append("/").append(tableName);
                if (partitionSpec != null)
                    sbDir.append("/").append(partitionSpec);
                newLocation = sbDir.toString();
            } else {
                // TODO: Shouldn't happen.
            }
        } else {
            switch (config.getDataStrategy()) {
                case EXPORT_IMPORT:
                case HYBRID:
                case SQL:
                case SCHEMA_ONLY:
                case DUMP:
                case STORAGE_MIGRATION:
                case CONVERT_LINKED:
                    newLocation = NamespaceUtils.replaceNamespace(originalLocation, rightNS);
                    break;
                case LINKED:
                case COMMON:
                    newLocation = originalLocation;
                    break;
            }
        }
        dirBuilder.append(newLocation);

        log.debug("Translate Table Location: {}: {}", originalLocation, dirBuilder);
        // Add Location Map for table to a list.
        // TODO: Need to handle RIGHT locations.
        if (config.getTransfer().getStorageMigration().isDistcp()
                && config.getDataStrategy() != DataStrategyEnum.SQL) {
            if (config.getDataStrategy() == DataStrategyEnum.STORAGE_MIGRATION) {
                config.getTranslator().addTranslation(dbName, Environment.LEFT, originalLocation, dirBuilder.toString().trim(), level);
            } else if (config.getTransfer().getStorageMigration().getDataFlow() == DistcpFlowEnum.PULL && !config.isFlip()) {
                config.getTranslator().addTranslation(dbName, Environment.RIGHT, originalLocation, dirBuilder.toString().trim(), level);
            } else {
                config.getTranslator().addTranslation(dbName, Environment.LEFT, originalLocation, dirBuilder.toString().trim(), level);
            }
        }

        return dirBuilder.toString().trim();
    }

    public void addGlobalLocationMap(String source, String target) throws SessionException {
        // Don't reload if running.
        executeSessionService.clearActiveSession();

        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();
        hmsMirrorConfig.getTranslator().addGlobalLocationMap(source, target);
    }

    public String removeGlobalLocationMap(String source) throws SessionException {
        // Don't reload if running.
        executeSessionService.clearActiveSession();

        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();
        return hmsMirrorConfig.getTranslator().removeGlobalLocationMap(source);
    }

    public Map<String, String> getGlobalLocationMap() {
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();
        return hmsMirrorConfig.getTranslator().getOrderedGlobalLocationMap();
    }

    public Map<String, String> buildGlobalLocationMapFromWarehousePlansAndSources(boolean dryrun, int consolidationLevel) throws MismatchException, SessionException {
        // Don't reload if running.
//        executeSessionService.clearActiveSession();

        HmsMirrorConfig config = executeSessionService.getSession().getConfig();
        Translator translator = config.getTranslator();
        Map<String, String> lclGlobalLocationMap = new TreeMap<>(new StringLengthComparator());

        WarehouseMapBuilder warehouseMapBuilder = translator.getWarehouseMapBuilder();
        Map<String, Warehouse> warehousePlans = warehouseMapBuilder.getWarehousePlans();
        Map<String, SourceLocationMap> sources = translator.getWarehouseMapBuilder().getSources();

        /*
         What do we have:
         1. Warehouse Plans (database, <external, managed>)
         2. Sources (database, <table_type, <location, tables>>)

        Build these by the database. So use the 'warehousePlans' as the base. For each warehouse plan
        database, get the external and managed locations.

        Now get the sources for the database.  For each *table type* go through the locations (reduce the location by the consolidation level).
            - The current level in the location is the database location, most likely. So by default, we should
                reduce the location by 1.
            - If the location is the equal to or starts with the warehouse location, then we don't need a glm for this.
            - If the location is different and they have specified `reset-to-default-location` then we need to add a glm
                for this that is the location (reduced by consolidation level) and the warehouse location.


        */

        for (Map.Entry<String, Warehouse> warehouseEntry : warehousePlans.entrySet()) {
            String database = warehouseEntry.getKey();
            Warehouse warehouse = warehouseEntry.getValue();
            String externalBaseLocation = warehouse.getExternalDirectory();
            String managedBaseLocation = warehouse.getManagedDirectory();
            SourceLocationMap locationMap = sources.get(database);
            if (locationMap != null) {
                for (Map.Entry<TableType, Map<String, Set<String>>> locationEntry : locationMap.getLocations().entrySet()) {
                    String typeLocation = null;
                    // Set the location based on the table type.
                    switch (locationEntry.getKey()) {
                        case EXTERNAL_TABLE:
                            typeLocation = externalBaseLocation;
                            break;
                        case MANAGED_TABLE:
                            typeLocation = managedBaseLocation;
                            break;
                    }
                    typeLocation = typeLocation + "/" + database + ".db";
                    // Locations and the tables that are in that location.
                    for (Map.Entry<String, Set<String>> locationSet : locationEntry.getValue().entrySet()) {
                        String location = new String(locationSet.getKey());
                        // String the namespace from the location.
                        location = NamespaceUtils.stripNamespace(location); //.replace(hmsMirrorConfig.getCluster(Environment.LEFT).getHcfsNamespace(), "");
                        if (config.getTransfer().getStorageMigration().isStrict() && (!location.startsWith("/") || (location.length() == locationSet.getKey().length()))) {
                            // Issue with reducing the location.
                            // This happens when the table(s) location doesn't match the source namespace.
                            throw new MismatchException("Location doesn't start with the configured namespace.  This is a problem"
                                    + " and doesn't allow for the location to be converted to the new namespace."
                                    + " Location: " + locationSet.getKey() + " Database: " + database + " Type: " + locationEntry.getKey()
                                    + " containing tables: " + String.join(",",locationSet.getValue())
                                    + " HCFS Namespace: " + config.getCluster(Environment.LEFT).getHcfsNamespace());
                        }
                        // TODO: Review Consolidation Level for this.
//                        String reducedLocation = UrlUtils.reduceUrlBy(location, consolidationLevel);
                        String reducedLocation = UrlUtils.reduceUrlBy(location, 0);
                        if (typeLocation != null) {
                            if (!location.startsWith(typeLocation)) {
                                lclGlobalLocationMap.put(reducedLocation, typeLocation);
                            }
                        }
                    }
                }
            }
        }
        if (!dryrun) {
            translator.setOrderedGlobalLocationMap(lclGlobalLocationMap);
        }
        return lclGlobalLocationMap;
    }
}
