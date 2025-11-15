/*
 * Copyright (c) 2025. Cloudera, Inc. All Rights Reserved
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


package com.cloudera.utils.hms.mirror.domain.core;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Schema(description = "Configuration for transferring ownership of databases and tables during migration")
public class TransferOwnership implements Cloneable {
    @Schema(description = "Transfer database ownership from source to target cluster")
    private boolean database = Boolean.FALSE;

    @Schema(description = "Transfer table ownership from source to target cluster")
    private boolean table = Boolean.FALSE;

    @Override
    public TransferOwnership clone() {
        try {
            return (TransferOwnership) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new AssertionError("Clone not supported for TransferOwnership", e);
        }
    }
}
