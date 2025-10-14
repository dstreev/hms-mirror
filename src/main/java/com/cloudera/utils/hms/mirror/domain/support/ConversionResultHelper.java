package com.cloudera.utils.hms.mirror.domain.support;

import com.cloudera.utils.hms.mirror.domain.core.DBMirror;
import com.cloudera.utils.hms.mirror.domain.core.EnvironmentTable;
import com.cloudera.utils.hms.mirror.domain.core.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.core.TableMirror;
import com.cloudera.utils.hms.util.TableUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.regex.Matcher;

@Slf4j
public class ConversionResultHelper {

    public static void removeWorkingData(ConversionResult original, HmsMirrorConfig config) {
        // Clean up the test data to match the configuration.
        for (DBMirror dbMirror : original.getDatabases().values()) {
            String database = dbMirror.getName();
            for (TableMirror tableMirror : dbMirror.getTableMirrors().values()) {
                EnvironmentTable et = tableMirror.getEnvironmentTable(Environment.LEFT);
                String tableName = tableMirror.getName();
                if (config.isDatabaseOnly()) {
                    // Only work with the database.
                    tableMirror.setRemove(true);
                } else if (TableUtils.isACID(et)
                        && !config.getMigrateACID().isOn()) {
                    tableMirror.setRemove(true);
                } else if (!TableUtils.isACID(et)
                        && config.getMigrateACID().isOnly()) {
                    tableMirror.setRemove(true);
                } else {
                    // Same logic as in TableService.getTables to filter out tables that are not to be processed.
                    if (tableName.startsWith(config.getTransfer().getTransferPrefix())) {
                        log.info("Database: {} Table: {} was NOT added to list.  The name matches the transfer prefix and is most likely a remnant of a previous event. If this is a mistake, change the 'transferPrefix' to something more unique.", database, tableName);
                        tableMirror.setRemove(true);
                        tableMirror.setRemoveReason("Table name starts with transfer prefix.");
                    } else if (tableName.endsWith(config.getTransfer().getStorageMigrationPostfix())) {
                        log.info("Database: {} Table: {} was NOT added to list.  The name is the result of a previous STORAGE_MIGRATION attempt that has not been cleaned up.", database, tableName);
                        tableMirror.setRemove(true);
                        tableMirror.setRemoveReason("Table name ends with storage migration postfix.");
                    } else {
                        if (config.getFilter().getTblRegEx() != null) {
                            // Filter Tables
                            assert (config.getFilter().getTblFilterPattern() != null);
                            Matcher matcher = config.getFilter().getTblFilterPattern().matcher(tableName);
                            if (!matcher.matches()) {
                                log.info("{}:{} didn't match table regex filter and will NOT be added to processing list.", database, tableName);
                                tableMirror.setRemove(true);
                            }
                        } else if (config.getFilter().getTblExcludeRegEx() != null) {
                            assert (config.getFilter().getTblExcludeFilterPattern() != null);
                            Matcher matcher = config.getFilter().getTblExcludeFilterPattern().matcher(tableName);
                            if (matcher.matches()) { // ANTI-MATCH
                                log.info("{}:{} matched exclude table regex filter and will NOT be added to processing list.", database, tableName);
                                tableMirror.setRemove(true);
                            }
                        }
                    }
                }
            }
        }

    }
}
