package com.cloudera.utils.hms.mirror.domain.support;

import com.cloudera.utils.hms.mirror.domain.core.HiveServer2Config;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.checkerframework.checker.units.qual.A;

import java.util.Arrays;
import java.util.List;

import static com.cloudera.utils.hms.mirror.domain.support.HiveDriverClasses.APACHE_HIVE_DRIVER;
import static com.cloudera.utils.hms.mirror.domain.support.PlatformType.*;

@Getter
@RequiredArgsConstructor
public enum DriverType {
//    APACHE_HIVE_1("Apache Hive 1",HiveServer2Config.APACHE_HIVE1_DRIVER,
//            List.of(APACHE_HIVE1),"hive/1"),
    APACHE_HIVE_2("Apache Hive 2", APACHE_HIVE_DRIVER,
            List.of(APACHE_HIVE2),"hive/2"),
    APACHE_HIVE_3("Apache Hive 3", APACHE_HIVE_DRIVER,
            List.of(EMR6,EMR7,APACHE_HIVE3),"hive/3"),
    APACHE_HIVE_4("Apache Hive 4", APACHE_HIVE_DRIVER,
            List.of(APACHE_HIVE4),"hive/4"),
    APACHE_HIVE_4_1("Apache Hive 4.1", APACHE_HIVE_DRIVER,
            List.of(APACHE_HIVE4),"hive/4_1"),
    HDP2_HIVE_DRIVER("HDP 2 Hive", APACHE_HIVE_DRIVER,
            List.of(HDP2),"hdp/2"),
    CDH_5_HIVE_DRIVER("CDH 5 Hive", APACHE_HIVE_DRIVER,
            List.of(CDH5),"cdh/5"),
    CDH_6_HIVE_DRIVER("CDH 6 Hive", APACHE_HIVE_DRIVER,
            List.of(CDH6),"cdh/6"),
    CDP_7_1_9_HIVE_DRIVER("CDP 7.1.9 Hive", APACHE_HIVE_DRIVER,
            List.of(HDP3, CDP7_0,CDP7_1,CDP7_1_9_SP1),"cdp/7_1_9"),
    CDP_7_3_1_HIVE_DRIVER("CDP 7.3.1 Hive", APACHE_HIVE_DRIVER,
            List.of(CDP7_2, CDP7_3),"cdp/7_3_1");

    @NonNull
    private final String name;
    @NonNull
    private final String driverClass;
    @NonNull
    private final List<PlatformType> platforms;
    @NonNull
    private final String path;

    /**
     * Find all DriverTypes that support the given PlatformType.
     *
     * @param platformType The platform type to search for
     * @return List of DriverTypes that support this platform, empty list if none found
     */
    public static List<DriverType> findByPlatformType(PlatformType platformType) {
        return Arrays.stream(DriverType.values())
                .filter(driverType -> driverType.getPlatforms().contains(platformType))
                .toList();
    }

}

