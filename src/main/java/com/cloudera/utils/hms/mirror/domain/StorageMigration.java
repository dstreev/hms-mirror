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

package com.cloudera.utils.hms.mirror.domain;

import com.cloudera.utils.hms.mirror.domain.support.DataMovementStrategyEnum;
import com.cloudera.utils.hms.mirror.domain.support.DistcpFlowEnum;
import com.cloudera.utils.hms.mirror.domain.support.TranslationTypeEnum;
import com.fasterxml.jackson.annotation.JsonIgnore;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Schema(description = "Storage migration configuration that controls the data movement strategy")
public class StorageMigration implements Cloneable {

    @Schema(description = "The type of translation to use to migrate locations")
    private TranslationTypeEnum translationType = TranslationTypeEnum.RELATIVE;
    @Schema(description = "Data movement strategy")
    private DataMovementStrategyEnum dataMovementStrategy = DataMovementStrategyEnum.SQL;
    @Schema(description = "Data flow direction for distcp. This control from where the 'distcp' jobs should be run.")
    private DistcpFlowEnum dataFlow = DistcpFlowEnum.PULL;
    @Schema(description = "When strict is true, any issues during evaluation will cause the migration to fail. When false, " +
            "the migration will continue but the issues will be reported. This can lead to data movement issues.")
    private boolean strict = Boolean.TRUE;

    @Override
    public StorageMigration clone() {
        try {
            StorageMigration clone = (StorageMigration) super.clone();
            // TODO: copy mutable state here, so the clone can't change the internals of the original
            return clone;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }

    @JsonIgnore
    public boolean isDistcp() {
        return dataMovementStrategy == DataMovementStrategyEnum.DISTCP;
    }
}
