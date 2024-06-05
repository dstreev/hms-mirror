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

package com.cloudera.utils.hms.mirror.domain;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;
import lombok.Setter;

/*
The Warehouse Base is the location base for a databases external and managedDirectory locations. The locations
should NOT include the database name.  The database name will be appended to the location when the process
is run.
 */
@Setter
@Getter
public class Warehouse implements Cloneable {
    @Schema(description = "The external directory location for the database.  This directory should NOT contain the database name, " +
            "the database name will be appended to the location when the process is run.")
    private String externalDirectory;
    @Schema(description = "The managed directory location for the database.  This directory should NOT contain the database name, " +
            "the database name will be appended to the location when the process is run.")
    private String managedDirectory;

    public Warehouse() {
    }

    public Warehouse(String externalDirectory, String managedDirectory) {
        this.externalDirectory = externalDirectory;
        this.managedDirectory = managedDirectory;
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
}
