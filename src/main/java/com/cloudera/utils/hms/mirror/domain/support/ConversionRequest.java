/*
 * Copyright (c) 2023-2025. Cloudera, Inc. All Rights Reserved
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

package com.cloudera.utils.hms.mirror.domain.support;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

@Getter
@Setter
@Slf4j
public class ConversionRequest implements Cloneable {

    /*
    A map of databases to the tables that need to be converted.
     */
    private Map<String, List<String>> databases = new TreeMap<>();

    @Override
    public ConversionRequest clone() {
        try {
            ConversionRequest clone = (ConversionRequest) super.clone();
            if (this.databases != null) {
                clone.databases = new TreeMap<>();
                for (Map.Entry<String, List<String>> entry : this.databases.entrySet()) {
                    clone.databases.put(entry.getKey(), new ArrayList<>(entry.getValue()));
                }
            }
            return clone;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError("Clone not supported for ConversionRequest", e);
        }
    }
}
