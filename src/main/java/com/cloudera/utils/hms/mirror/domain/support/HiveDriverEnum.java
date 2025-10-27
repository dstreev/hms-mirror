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


package com.cloudera.utils.hms.mirror.domain.support;

import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static com.cloudera.utils.hms.mirror.domain.support.HiveDriverClasses.APACHE_HIVE_DRIVER;
import static com.cloudera.utils.hms.mirror.domain.support.HiveDriverClasses.CLOUDERA_HIVE_DRIVER;

@Slf4j
public enum HiveDriverEnum {

    APACHE_HIVE(APACHE_HIVE_DRIVER, Arrays.asList("host", "port",
            "dbName", "sessionConfs", "hiveConfs", "hiveVars", " transportMode", "principal", "saslQop", "user",
            "password", "ssl", "sslTrustStore", "trustStorePassword", "user", "password", "serviceDiscoveryMode",
            "zookeeperKeyStoreType", "zookeeperNamespace", "retries")),
    CLOUDERA_HIVE(CLOUDERA_HIVE_DRIVER, Arrays.asList("AllowSelfSignedCerts",
            "AsyncExecPollInterval","AuthMech","BinaryColumnLength","CAIssuedCertsMismatch","CatalogSchemaSwitch",
            "DecimalColumnScale","DefaultStringColumnLength","DelegationToken","DelegationUID",
            "FastConnection","httpPath","JWTString","IgnoreTransactions", "KrbAuthType",
            "KrbHostFQDN","KrbRealm","KrbServiceName","LoginTimeout",
            "LogLevel","LogPath","NonRowcountQueryPrefixes","PreparedMetaLimitZero","PWD",
            "RowsFetchedPerBlock","SocketTimeout","SSL","SSLKeyStore","SSLKeyStoreProvider",
            "SSLKeyStorePwd","SSLKeyStoreType","SSLTrustStore","SSLTrustStoreProvider",
            "SSLTrustStorePwd","SSLTrustStoreType","TransportMode", "user", "password",
            "UID","UseNativeQuery","zk"));


    private final String driverClassName;
    // A list of supported driver parameters
    private final List<String> driverParameters;

    HiveDriverEnum(String driverClassName, List<String> driverParameters) {
        this.driverClassName = driverClassName;
        this.driverParameters = driverParameters;
    }

    public String getDriverClassName() {
        return driverClassName;
    }

    public List<String> getDriverParameters() {
        return driverParameters;
    }

    public static String[] getDriverClassNames() {
        return Arrays.stream(HiveDriverEnum.values())
                .map(HiveDriverEnum::getDriverClassName)
                .toArray(String[]::new);
    }

    public static HiveDriverEnum getDriverEnum(String driverClassName) {
        return Arrays.stream(HiveDriverEnum.values())
                .filter(driver -> driver.getDriverClassName().equals(driverClassName))
                .findFirst()
                .orElse(null);
    }

    public static List<String> getDriverParameters(String driverClassName) {
        return Arrays.stream(HiveDriverEnum.values())
                .filter(driver -> driver.getDriverClassName().equals(driverClassName))
                .findFirst()
                .map(HiveDriverEnum::getDriverParameters)
                .orElse(null);
    }

    /**
     * Remove any properties that aren't allowed by the driver.  Not allowed is defined as not being one of the
     * properties in this class.
     *
     * @param properties
     * @return
     */
    public Properties reconcileForDriver(Properties properties) {
        Properties reconciledProperties = new Properties();
        for (String key : properties.stringPropertyNames()) {
            if (driverParameters.contains(key)) {
                reconciledProperties.put(key, properties.getProperty(key));
            } else {
                // TODO: Add and Warn OR omit and LOG.
                log.warn("Property " + key + " is not supported by driver " + driverClassName);
            }
        }
        return reconciledProperties;
    }

}