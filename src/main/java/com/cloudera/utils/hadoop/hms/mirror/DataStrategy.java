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

package com.cloudera.utils.hadoop.hms.mirror;

import java.util.ArrayList;
import java.util.List;

public enum DataStrategy {
    /*
    The DUMP strategy will run against the LEFT cluster (attaching to it) and build scripts
     based on the configuration in that cluster.  You can optionally use the -ds|--dump-source
     option to use the RIGHT cluster as the target for the dump.
     */
    DUMP(Boolean.FALSE),
    /*
    This will transfer the schema only, replace the location with the RIGHT
    clusters location namespace and maintain the relative path.
    The data is transferred by an external process, like 'distcp'.
     */
    SCHEMA_ONLY(Boolean.FALSE),
    /*
    Assumes the clusters are LINKED.  We'll transfer the schema and leave
    the location as is on the new cluster.  This provides a means to test
    Hive on the RIGHT cluster using the LEFT clusters storage.
     */
    LINKED(Boolean.FALSE),
    /*
    Assumes the clusters are LINKED.  We'll use SQL to migrate the data
    from one cluster to another.
     */
    SQL(Boolean.FALSE),
    /*
    Assumes the clusters are LINKED.  We'll use EXPORT_IMPORT to get the
    data to the new cluster.  EXPORT to a location on the LEFT cluster
    where the RIGHT cluster can pick it up with IMPORT.
     */
    EXPORT_IMPORT(Boolean.FALSE),
    /*
    Hybrid is a strategy that select either SQL or EXPORT_IMPORT for the
    tables data strategy depending on the criteria of the table.
     */
    HYBRID(Boolean.FALSE),
    /*
    If you had previously created a schema on the RIGHT that is LINKED to the LEFT's data,
    this option provides a way to 'convert' the LINKED schemas to SCHEMA_ONLY without having to
    drop and recreate the schema.  This assumes that the data is migrated through other means.
    The conversion will simple review the RIGHT schema's and convert the NAMESPACE from the LEFT
    to the RIGHT's NAMESPACE.  In addition, tables that were 'legacy' managed and converted to EXTERNAL
    WITHOUT purge, will have the purge flag set since the data is now owned by the RIGHT cluster.
     */
    CONVERT_LINKED(Boolean.FALSE),
    /*

     */
    STORAGE_MIGRATION(Boolean.FALSE),
    /*
    The data storage is shared between the two clusters and no data
    migration is required.  Schema's are transferred using the same
    location.  Commit/ownership?
     */
    COMMON(Boolean.FALSE),
    /*
    ACID Transfer is a hidden strategy applied to the table.
     */
    ACID(Boolean.TRUE);

    private boolean hidden;

    DataStrategy(boolean hidden) {
        this.hidden = hidden;
    }

    public static DataStrategy[] visibleValues() {
        List<DataStrategy> dsList = new ArrayList<DataStrategy>();
        for (DataStrategy dataStrategy: values()) {
            if (!dataStrategy.hidden) {
                dsList.add(dataStrategy);
            }
        }
        DataStrategy[] rtn = dsList.toArray(new DataStrategy[0]);
        return rtn;
    }
}
