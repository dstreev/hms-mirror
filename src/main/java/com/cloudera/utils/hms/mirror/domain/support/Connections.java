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

package com.cloudera.utils.hms.mirror.domain.support;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.TreeMap;

@Getter
@Setter
@Slf4j
public class Connections implements Cloneable {

    private Map<Environment, Connection> hiveServer2Connections = new TreeMap<>();
    private Map<Environment, Connection> metastoreDirectConnections = new TreeMap<>();
    private Map<Environment, Connection> namespaces = new TreeMap<>();


    public Connections() {
        hiveServer2Connections.put(Environment.LEFT, new Connection(Environment.LEFT));
        hiveServer2Connections.put(Environment.RIGHT, new Connection(Environment.RIGHT));
        metastoreDirectConnections.put(Environment.LEFT, new Connection(Environment.LEFT));
        metastoreDirectConnections.put(Environment.RIGHT, new Connection(Environment.RIGHT));
        namespaces.put(Environment.LEFT, new Connection(Environment.LEFT));
        namespaces.put(Environment.RIGHT, new Connection(Environment.RIGHT));
        namespaces.put(Environment.TARGET, new Connection(Environment.TARGET));
    }

    public void reset() {
        hiveServer2Connections.values().forEach(Connection::reset);
        metastoreDirectConnections.values().forEach(Connection::reset);
        namespaces.values().forEach(Connection::reset);
    }

    public Connections clone() {
        try {
            Connections connections = (Connections) super.clone();
            return connections;
        } catch (CloneNotSupportedException e) {
            log.error("Clone not supported", e);
            throw new RuntimeException(e);
        }
    }
}
