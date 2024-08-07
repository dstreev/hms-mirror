/*
 * Copyright (c) 2023-2024. Cloudera, Inc. All Rights Reserved
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

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public enum MessageCode {
    // ERRORS
    MISC_ERROR(1, "Check log for details. Miscellaneous Error: {0}"),
    ACID_NOT_TOP_LEVEL_STRATEGY(2, "The `ACID` strategy is not a valid `hms-mirror` top level strategy.  Use 'HYBRID' or 'SQL' " +
            "along with the `-ma|-mao` option to address ACID tables."),
    COMMON_STORAGE_WITH_LINKED(3, "Common Storage (-cs) is not a valid option for the LINKED data strategy."),
    INTERMEDIATE_STORAGE_WITH_LINKED(4, "Intermediate Storage (-is) is not a valid option for the LINKED data strategy."),
    LINK_TEST_FAILED(5, "Link test failed.  Use -slc|--skip-link-check to bypass."),
    LEGACY_HIVE_RIGHT_CLUSTER(6, "Legacy Hive is not supported as a 'target' (RIGHT) cluster.  clusters->RIGHT->legacyHive"),
    DOWNGRADE_ONLY_FOR_ACID(7, "-da option can only be used for ACID tables. (-ma|-mao)"),
    REPLACE_ONLY_WITH_SQL(8, "-r option can only be used with the SQL data strategy. (-d SQL)"),
    REPLACE_ONLY_WITH_DA(9, "-r option when used with -ma|-mao (migrate ACID) must be used with -da (downgrade-acid)."),
    LEFT_HS2_URI_INVALID(10, "LEFT HiveServer2 URI config is NOT valid"),
    LEFT_KERB_JAR_LOCATION(11, "LEFT: For Kerberized connections, place the Hive JDBC jar file in $HOME/.hms-mirror/aux_libs and remove the 'jarFile' entry in the config."),
    RIGHT_HS2_URI_INVALID(12, "The RIGHT HiveServer2 URI is not defined OR invalid. You need to define the RIGHT cluster " +
            "with a valid URI for all Data Strategies, except DUMP"),
    RIGHT_KERB_JAR_LOCATION(13, "RIGHT: For Kerberized connections, place the Hive JDBC jar file in $HOME/.hms-mirror/aux_libs and remove the 'jarFile' entry in the config."),
    KERB_ACROSS_VERSIONS(14, "Kerberos connections can only be supported to a single version of the platform.  LEFT and RIGHT " +
            "'legacy' definitions are not the same, so we are assuming the cluster versions aren't the same."),
    TARGET_DB_MISSING(15, ""),
    RO_DB_DOESNT_EXIST(16, "Database directory: ** {0} ** on the RIGHT cluster does NOT exist.\n" +
            "In read-only mode, it must exist before creating the database to ensure we " +
            "don`t corrupt the Filesystems `Read-Only` State.\n" +
            "> ErrCode: {1}\n" +
            "> HCFS Command: {2}\n" +
            "WARNING: If you have created the Database on the RIGHT cluster already, " +
            "it is possible that the DB property LOCATION (external warehouse) does " +
            "NOT match to DB location on the LEFT.  They MUST match to process `--read-only`.\n" +
            "You can either DROP the right database and run hms-mirror with the `-dbo -e` " +
            "option OR \"ALTER DATABASE {3} SET LOCATION `{0}`\""),
    CONFIGURATION_REMOVED_OR_INVALID(17, "A configuration element is no longer valid, progress.  Please remove the element from the configuration yaml and try again. {0}"),
    STORAGE_MIGRATION_REQUIRED_NAMESPACE(18, "STORAGE_MIGRATION requires -smn or -cs to define the new namespace."),
    STORAGE_MIGRATION_REQUIRED_STRATEGY(19, "STORAGE_MIGRATION requires -sms to set the Data Strategy.  Applicable options " +
            "are SCHEMA_ONLY, SQL, EXPORT_IMPORT, or HYBRID"),
//    STORAGE_MIGRATION_REQUIRED_WAREHOUSE_OPTIONS(20, "STORAGE_MIGRATION requires one of to things.  Either you set the " +
//            "global warehouse locations (warehouse-directory and external-warehouse-directory) or you define some " +
//            "global location maps that will make the translations."),
    RIGHT_HS2_DEFINITION_MISSING(21, "The 'RIGHT' HS2 definition is missing.  Only STORAGE_MIGRATION or DUMP strategies allow " +
            "that definition to be skipped."),

    RESET_TO_DEFAULT_LOCATION(22, "'reset-to-default-location' is NOT available for this data strategy: {0}."),
    DISTCP_VALID_STRATEGY(23, "The `distcp` option is not valid for this strategy ({0}) and configuration."),
    ALIGNED_DISTCP_EXECUTE(24, "STORAGE_MIGRATION with 'distcp' requires MANUAL intervention to run " +
            "'distcp' to migrate the data separately.  This process expects the data to be migrated already.  "),
    STORAGE_MIGRATION_DISTCP_ACID(25, "STORAGE_MIGRATION with 'distcp' can't support the direct transfer of ACID tables without -epl."),
    ACID_DOWNGRADE_SCHEMA_ONLY(26, "Use the 'SQL' data-strategy to 'downgrade' an ACID table with 'distcp'"),
    OPTIONAL_ARG_ISSUE(27, "Bad optional argument"),
    CONNECTION_ISSUE(28, "JDBC connections issue.  Check environment, jdbc urls, libraries, etc."),
    NON_LEGACY_TO_LEGACY(29, "`hms-mirror` does NOT support migrations from Hive 3 to Hive 1/2."),
    //    DISTCP_VALID_DISTCP_RESET_TO_DEFAULT_LOCATION(30, "You must specify `-wd` and `-ewd` when using `-rdl` with `--distcp`."),
    ALIGN_LOCATIONS_WITHOUT_WAREHOUSE_PLANS(30, "When using `ALIGNED`, you will need specify " +
            "warehouse locations (-wd,-ewd) OR have a Warehouse Plan created for the 'database' to enable the `distcp` workbooks and/or resetting locations.  Without them, we can NOT know the " +
            "default locations to build a plan."),
    SQL_ACID_DA_DISTCP_WO_EXT_WAREHOUSE(31, "You need to specify `-ewd` when using `distcp`, `da`, and `SQL`"),
    SQL_DISTCP_ONLY_W_DA_ACID(32, "SQL Strategy with `distcp` is only valid for Downgraded (-da) ACID table transfers.  " +
            "Use SCHEMA_ONLY from External and Legacy Managed (Non-Transactional) tables."),
    SQL_DISTCP_ACID_W_STORAGE_OPTS(33, "SQL Strategy with `distcp` is only valid for ACID table transfers NOT using " +
            "storage options `-is` or `-cs`.  `distcp` is NOT required since the data has already been moved while preparing " +
            "the ACID table."),
    DISTCP_W_RELATIVE(34, "Using `distcp` with RELATIVE translations will NOT inspect all table and partition locations.  The plan " +
            "built will be at the database level only.  Any tables or partitions not following standard schema locations may not " +
            "get translated correctly, resulting in potential data loss."),
    ENCRYPT_PASSWORD_ISSUE(35, "Issue Encrypting PasswordApp"),
    DECRYPTING_PASSWORD_ISSUE(36, "Issue decrypting password(s)"),
    PKEY_PASSWORD_CFG(37, "Passwords are encrypted.  You must supply a password key to run the process. For CLI '-pkey' with '-p'."),
    PASSWORD_CFG(38, "PasswordApp en/de crypt"),
    VALID_ACID_DA_IP_STRATEGIES(39, "Inplace Downgrade of ACID tables only valid for the SQL data strategy"),
    COMMON_STORAGE_WITH_DA_IP(40, "Common Storage (-cs) is not a valid option for the ACID downgrades inplace."),
    INTERMEDIATE_STORAGE_WITH_DA_IP(41, "Intermediate Storage (-is) is not a valid option for the ACID downgrades inplace."),
    DISTCP_W_DA_IP_ACID(42, "`distcp` is not valid for Downgraded (-da) ACID table in-place (-ip)."),
    DA_IP_NON_LEGACY(43, "ACID Inplace Downgrade only works on Non-Legacy Hive. IE: Hive3+"),
    DISTCP_REQUIRED_FOR_SCHEMA_ONLY_RDL(44, "You need to add `--distcp` to options when 'resetting' default " +
            "location with SCHEMA_ONLY so you can build out the movement plan."),
    DISTCP_REQUIRED_FOR_SCHEMA_ONLY_IS(45, "You need to add `--distcp` to options when using `-is` " +
            "with SCHEMA_ONLY so you can build out the movement plan."),
    SAME_CLUSTER_COPY_WITHOUT_RDL(46, "You must specify `-rdl` when the cluster configurations use the same storage."),
    SAME_CLUSTER_COPY_WITHOUT_DBPR(47, "You must specify `-dbp` or `-dbr` when the cluster configurations use the same storage."),
    DB_RENAME_ONLY_WITH_SINGLE_DB_OPTION(48, "DB Rename can only be used for a single DB `-db`."),
    ENVIRONMENT_CONNECTION_ISSUE(49, "There is an issue connecting to the {0} HS2 environment.  Check jdbc setup."),
    // WARNINGS
    SYNC_TBL_FILTER(50, "'sync' with 'table filter' will be bi-directional ONLY for tables that meet the table filter '"
            + "' ON BOTH SIDES!!!"),
    LINK_TEST_SKIPPED_WITH_IS(51, "Link TEST skipped because you've specified either 'Intermediate Storage' or 'Common Storage' option"),
    DUMP_ENV_FLIP(52, "You've requested DUMP on the RIGHT cluster.  The runtime configuration will " +
            "adjusted to complete this.  The RIGHT configuration will be MOVED to the LEFT to process " +
            "the DUMP strategy.  LEFT = RIGHT..."),
    ALIGN_LOCATIONS_WARNING(53, "'ALIGN' was specified.  Table definition stripped of " +
            "LOCATION.  Location will be determined by the database or system warehouse settings."),
    DISTCP_OUTPUT_NOT_REQUESTED(54, "To get the `distcp` workplans add `-dc|--distcp` to commandline."),
    DISTCP_RDL_WO_WAREHOUSE_DIR(55, "When using `-rdl|--reset-to-default-location` you must also specify " +
            "warehouse locations `-wd|-ewd` to build the `distcp` workplans."),
    ENCRYPTED_PASSWORD(56, "Encrypted password: {0}"),
    DECRYPTED_PASSWORD(57, "Decrypted password: {0}"),
    ENVIRONMENT_DISCONNECTED(58, "Environment {0} is disconnected. Current db/table status could not be determined.  " +
            "All actions will assume they don't exist.\n\nStrategies/methods of sync that require the 'RIGHT' cluster or 'LEFT' cluster " +
            "to be linked may not work without a `common-storage` or `intermediate-storage` option that will bridge the gap."),
    RELATIVE_MANUAL(59, "You may NOT be able to migrate non-standard locations for table/partition datasets " +
            "using the 'RELATIVE' translation type with a 'MANUAL' data movement strategy. " +
            "It's recommended to use the 'ALIGNED' translation type with the 'DISTCP' data movement strategy " +
            "along with 'Warehouse Plans' and possibly some specific 'Global Location Maps' to address non-standard locations " +
            "in order to build a complete data movement plan."),
    //    RDL_DC_WARNING_TABLE_ALIGNMENT(59, "Using the options `-dc` and `-rdl` together may yield some inconsistent results. " +
//            "__If the 'current' table locations don't match the table name__, `distcp` will NOT realign those directories to the " +
//            "table names.  Which means the adjusted tables may not align with the directories. See: [Issue #35](https://github.com/cloudera-labs/hms-mirror/issues/35) " +
//            "for work going on to address this."),
//    STORAGE_MIGRATION_NAMESPACE_LEFT(60, "You didn't specify -smn or -cs for STORAGE_MIGRATION.  We're assuming you are migrating " +
//            "within the same namespace {0}."),
    STORAGE_MIGRATION_NAMESPACE_LEFT_MISSING_RDL_GLM(61, "You're using the same namespace in STORAGE_MIGRATION, without `-rdl` you'll need to " +
            "ensure you have `-glm` set to map locations."),
    TABLE_LOCATION_REMAPPED(62, "The tables location matched one of the 'global location map' directories. " +
            "The LOCATION element was adjusted and will be explicitly set during table creation."),
    TABLE_LOCATION_FORCED(63, "You've request the table location be explicitly set."),
    HDPHIVE3_DB_LOCATION(64, "HDP3 Hive did NOT have a MANAGEDLOCATION attribute for Databases.  The LOCATION " +
            "element tracked the Manage ACID tables and will control where they go.  This LOCATION will need to be transferred to " +
            "MANAGEDLOCATION 'after' upgrading to CDP to ensure ACID tables maintain the same behavior.  EXTERNAL tables will " +
            "explicity set there LOCATION element to match the setting in `-ewd`.  Future external tables, when no location is " +
            "specified, will be created in the `hive.metastore.warehouse.external.dir`.  This value is global in HDP Hive3 and " +
            "can NOT be set for individual databases.  Post upgrade to CDP, you should add a specific directory value at the " +
            "database level for better control."),
    HDP3_HIVE(65, "You've specified the cluster as an HDP3 Hive cluster.  This version of Hive has some issues regarding locations.  " +
            "We will need to specifically set the location for tables.  This will be done automatically by applying the `-fel` flag " +
            "for the cluster."),
    LINKED_NO_ACID_SUPPORT(66, "Can't LINK ACID tables.  ma|mao options are not valid with LINKED data strategy."),
//    EVALUATE_PARTITION_LOCATION(66, "Evaluating Partition Details is a resource intensive operation. " +
//            "This may have an impact on the Hive Metastore's RDBMS."),
//    EVALUATE_PARTITION_LOCATION_USE(67, "The `-epl|--evaluate-partition-location` flag is only valid for DUMP, SCHEMA_ONLY and STORAGE_MIGRATION strategies.  Remove the " +
//            "flag for all other strategies."),
//    EVALUATE_PARTITION_LOCATION_CONFIG(68, "The metastore_direct is not configured for the {0} cluster.  It is required when using " +
//            "`-epl|--evaluate-partition-location`."),
//    EVALUATE_PARTITION_LOCATION_STORAGE_MIGRATION(69, "The `-epl|--evaluate-partition-location` flag is only valid when `-dc` option is " +
//            "specified for STORAGE_MIGRATION strategy."),
    HIVE3_ON_HDP_ACID_TRANSFERS(70, "Hive3 on HDP does NOT honor the 'database' LOCATION element for newly created MANAGED tables. " +
            "For STORAGE_MIGRATION in the same metastore/namespace, move the data with `distcp` and ALTER the table locations by adding " +
            "`-dc` option to the commandline."),
    IGNORING_TBL_FILTERS_W_TEST_DATA(71, "Table filters are ignored with 'test data'"),
    DISTCP_W_TABLE_FILTERS(72, "`distcp` workbooks will include the current table directories and build separate " +
            "`distcp` plans for each table to contain/manage the data transferred to the tables directory (and possible non-standard " +
            "directories caught by partition locations)"),
    DISTCP_WO_TABLE_FILTERS(73, "`distcp` workbooks will include the DATABASE base directory, which may include more data than is " +
            "expected at the 'db' level, especially if the root directory is used by other processes outside of the tables " +
            "defined in the database."),
    RDL_W_EPL_NO_MAPPING(74, "`-epl` with `-rdl` requires a 'Warehouse Plan' for the database or a manual 'Global Location Map' to ensure we can map from the original location " +
            "to the new location.  Source: {0} : {1} "),
    LOCATION_NOT_MATCH_WAREHOUSE(75, "After all the translations, the `{0}` " +
            "location is still NOT aligned in the DBs warehouse. `{1}`->`{2}`. Consider adding a warehouse plan, a glm mapping, or resetting to default locations to align them"),
    DISTCP_FOR_SO_ACID(76, "`distcp` can NOT be used to migrate data for ACID tables.  Try using strategies: " +
            "SQL, EXPORT_IMPORT, or HYBRID"),
    SQL_ACID_W_DC(77, "`distcp` isn't valid for SQL strategies on ACID tables."),
    FLIP_WITHOUT_RIGHT(80, "You can use the 'flip' option if there isn't a RIGHT cluster defined in the configuration."),
    WAREHOUSE_DIRS_SAME_DIR(81, "You can't use the same location for EXTERNAL {0} and MANAGED {1} warehouse locations."),
    COLLECTING_TABLE_DEFINITIONS(82, "There was an issue collecting table definitions.  Please check logs."),
    DATABASE_CREATION(83, "There was an issue creating/modifying databases.  Ensure that the warehouse directories have been set.  Check logs."),
    COLLECTING_TABLES(84, "There was an issue collecting tables.  Please check logs."),
    LEGACY_AND_HIVE3(85, "Setting legacyHive=true and hdpHive3=true is a conflicting configuration"),
    RDL_FEL_OVERRIDES(86, "You've request both -rdl and -fel. -fel will take precedence."),
    CINE_WITH_DATASTRATEGY(87, "The `-cine` (createIfNotExist) option is only applied while using the `SCHEMA_ONLY` data strategy " +
            "OR with SQL and the `-sync` option."),
    CINE_WITH_EXIST(88, "Schema exists already.  But you've specified 'createIfNotExist', which will attempt to create " +
            "and softly fail and continue with the remainder sql statements for the table."),
    SCHEMA_EXISTS_NO_ACTION(89, "Schema exists already and matches. No action necessary"),
    SQL_SYNC_WO_CINE(90, "Sync NOT supported with the SQL strategy without the `-cine|create-if-not-exist` flag."),
    SQL_SYNC_W_CINE(90, "The schema already exists and you've asked for 'sync'.  The target tables schema will remain in" +
            "place and the tables data will be overwritten via SQL. NOTE: If the table is partitioned and source partitions " +
            "are deleted, those will NOT be removed through this process and may lead to data inconsistencies.  If this is an " +
            "issue, you should 'drop' the target table and restart the process to get a clean bootstrap of the data."),
    SCHEMA_WILL_BE_CREATED(91, "Schema will be created"),
    EXPORT_IMPORT_SYNC(90, "Schema EXISTS in target.  Table will be 'dropped' before IMPORT attempt.  If the table " +
            "isn't ACID or EXTERNAL/PURGE, existing data may prevent the RE-CREATION of the table when script is executed."),
    VALID_SYNC_STRATEGIES(91, "'--sync' only valid for SCHEMA_ONLY, LINKED, SQL, EXPORT_IMPORT, HYBRID, and COMMON data strategies"),
    VALID_ACID_STRATEGIES(92, "Migrating ACID tables only valid for SCHEMA_ONLY, DUMP, SQL, EXPORT_IMPORT, " +
            "HYBRID, and STORAGE_MIGRATION data strategies"),
    SCHEMA_EXISTS_NO_ACTION_DATA(93, "Schema exists already. Drop it and " +
            "try again or add `--sync` to OVERWRITE current tables data."),
    SCHEMA_EXISTS_SYNC_PARTS(94, "Schema exists already and matches. `--sync` and `-epl` specified, adding partition sync."),
    SCHEMA_EXISTS_SYNC_ACID(95, "Schema already exists.  You've specified '--sync', the target table will be dropped and " +
            "re-created.  The data will be overwritten."),
    RO_VALID_STRATEGIES(96, "Read-Only (RO) option only valid with SCHEMA_ONLY, LINKED, SQL, and COMMON data strategies."),
    DECRYPTED_CONFIG_PASSWORDS(97, "Decrypted Config Passwords"),
    DECRYPTING_CONFIG_PASSWORDS_ISSUE(98, "Issue decrypting config passwords"),
    STORAGE_MIGRATION_STRICT(99, "Storage Migration is in 'strict' mode.  If the table and/or partition locations can't "
            + "be mapped to the warehouse locations or there are mismatches in location standards, we can't process the table "
            + "without potential data loss.  Add additional 'global-location-map' entries to cover the locations."
            + "Mismatched directories or non-standard partition locations can only be handled through the " +
            "SQL dataMovementStategy."),
//    METASTORE_DIRECT_CONFIG(100, "The metastore_direct is not configured for the {0} cluster.  It is required when using " +
//            "`-epl|--evaluate-partition-location` and/or the STORAGE_MIGRATION datastrategy. Without it we can't efficiently " +
//            "evaluate the partition locations or build the storage migration plans."),
    STORAGE_MIGRATION_NOT_AVAILABLE_FOR_LEGACY(101, "Storage Migration is NOT available for Legacy Hive."),
    DISTCP_WITH_MISMATCHING_LOCATIONS(102, "You''ve specified ''distcp'' with mismatching locations for {0} {1}: " +
            "Original Location {2}, Specification {3}. When these don''t match, a valid distcp plan can''t be created to " +
            "correctly align the data elements.  You''ll need to use SQL to migrate the data and allow Hive to reorganize it " +
            "according to your specs."),
    DISTCP_WITH_MISMATCHING_TABLE_LOCATION(103, "You''ve specified ''distcp'' with mismatching locations for {0} {1}: " +
            "Original Location: {2}, Derived table name from directory: {3}. The partition directory doesn''t match the table name and we can''t " +
            "correctly align the data elements via distcp.  You''ll need to use SQL to migrate the data and allow Hive to reorganize it " +
            "according to your specs."),
    CLUSTER_NOT_DEFINED_OR_CONFIGURED(104, "The {0} cluster is NOT defined or configured in the runtime configuration.  " +
            "Please check the configuration and try again."),
    HS2_NOT_DEFINED_OR_CONFIGURED(105, "The {0} HiveServer2 is NOT defined or configured in the runtime configuration.  " +
            "Please check the configuration and try again."),
    METASTORE_DIRECT_NOT_DEFINED_OR_CONFIGURED(106, "The {0} metastore_direct is NOT defined or configured in the runtime configuration.  " +
            "Please check the configuration and try again."),
    MISSING_PROPERTY(107, "The property ''{0}'' is missing from the configuration {1} {2}.  Please add it and try again."),
    WAREHOUSE_DIRECTORIES_NOT_DEFINED(108, "The warehouse directories are NOT defined.  " +
            "Please add them and try again."),
    WAREHOUSE_DIRECTORIES_RETRIEVED_FROM_HIVE_ENV(109, "The warehouse directories were retrieved from the Hive environment.  If these are not the intended " +
            "directories, add the warehouse directories to the configuration and try again."),
    STORAGE_MIGRATION_GLMS_NOT_BUILT(110, "The global location maps (GLMs) have not been built yet.  We found warehouse definitions but no GLM's.  " +
            "Please build or create GLM's and try again."),
    ENCRYPTED_PASSWORD_CHANGE_ATTEMPT(111, "Password can NOT be changed while ENCRYPTED.  Decrypt them first to change/add a password then Re-encrytped them before saving."),
    PASSWORDS_ENCRYPTED(112,"Passwords are encrypted.  Too change/add a password, you must decrypt them first."),
    LEFT_NAMESPACE_NOT_DEFINED(113, "The namespace for the LEFT cluster is NOT defined.  Please config it and try again."),
    RIGHT_NAMESPACE_NOT_DEFINED(114, "The namespace for the RIGHT cluster is NOT defined.  Please config it and try again."),

    RUNTIME_EXCEPTION(116, "A runtime exception occurred. ''{0}'' Check the logs for details."),
    SESSION_ISSUE(117, "There was an issue with the session. ''{0}'' Check the logs for details."),
    ENCRYPTION_ISSUE(118, "There was an issue with the encryption. ''{0}'' Check the logs for details."),
    TARGET_NAMESPACE_NOT_DEFINED(119, "Unable to determine the Target Namespace. Check the settings for the RIGHT hcfsNamespace or set the 'targetNamespace'"),
    DISTCP_W_RENAME_NOT_SUPPORTED(120, "DISTCP with Rename is NOT supported. Change Data Movement or remove 'rename' option (dbPrefix or deRename)"),
    RESET_TO_DEFAULT_LOCATION_REQUIRED_FOR_STORAGE_MIGRATION(121, "You must specify `-rdl` when using STORAGE_MIGRATION to ensure the locations are correctly aligned. To override this, set 'strict' to FALSE"),
    WAREHOUSE_PLANS_REQUIRED_FOR_STORAGE_MIGRATION(122, "You must specify a warehouse plan when using STORAGE_MIGRATION to ensure the locations are correctly aligned.");


    private int code = 0;
    private String desc = null;

    MessageCode(int code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public static List<MessageCode> getCodes(BitSet bitSet) {
        List<MessageCode> errors = new ArrayList<MessageCode>();
        for (int i = 0; i < bitSet.size(); i++) {
            if (bitSet.get(i)) {
                for (MessageCode error : MessageCode.values()) {
                    if (error.getCode() == i) {
                        errors.add(error);
                    }
                }
            }
        }
        return errors;
    }

    public static long getCheckCode(MessageCode... messageCodes) {
        int check = 0;
        BitSet bitSet = new BitSet(150);
        long expected = 0;
        for (MessageCode messageCode : messageCodes) {
            bitSet.set(messageCode.getCode());
        }
        long[] messageSet = bitSet.toLongArray();
        for (long messageBit : messageSet) {
            expected = expected | messageBit;
        }
        // Errors should be negative return code.
        return expected * -1;
    }

    public int getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }

    public long getLong() {
        double bitCode = Math.pow(2, code);
        return (long) bitCode;
    }

}
