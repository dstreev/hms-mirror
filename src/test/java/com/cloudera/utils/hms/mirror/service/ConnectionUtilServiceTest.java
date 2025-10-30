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

package com.cloudera.utils.hms.mirror.service;

import com.cloudera.utils.hms.mirror.domain.core.HiveServer2Config;
import com.cloudera.utils.hms.mirror.domain.dto.ConnectionDto;
import com.cloudera.utils.hms.mirror.domain.support.DriverType;
import com.cloudera.utils.hms.mirror.domain.support.PlatformType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static com.cloudera.utils.hms.mirror.domain.support.HiveDriverClasses.*;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class for ConnectionUtilService.
 * Validates driver discovery and classpath building for all PlatformTypes and DriverTypes.
 */
public class ConnectionUtilServiceTest {

    @TempDir
    private Path tempDriverDir;

    private DriverUtilsService driverUtilsService;
    private ConnectionUtilService connectionUtilService;

    @BeforeEach
    void setUp() throws IOException {
        // Create and configure DriverUtilsService
        driverUtilsService = new DriverUtilsService();
        driverUtilsService.setDriverBaseDirectory(tempDriverDir.toString());
        driverUtilsService.init();

        // Create ConnectionUtilService with the configured DriverUtilsService
        connectionUtilService = new ConnectionUtilService(driverUtilsService);

        // Create test driver directory structure and mock JAR files
        createTestDriverStructure();
    }

    /**
     * Creates a realistic driver directory structure with mock JAR files
     * matching the structure expected by DriverType paths.
     */
    private void createTestDriverStructure() throws IOException {
        // Apache Hive drivers
        createDriverJars("hive/2", "hive-jdbc-2.3.10-standalone.jar", "hadoop-common-2.10.0.jar",
                "hadoop-auth-2.10.0.jar", "log4j-api-2.18.0.jar", "log4j-core-2.18.0.jar");
        createDriverJars("hive/3", "hive-jdbc-3.1.3-standalone.jar", "hadoop-common-3.1.1.jar",
                "hadoop-auth-3.1.1.jar", "log4j-api-2.18.0.jar", "log4j-core-2.18.0.jar");
        createDriverJars("hive/4", "hive-jdbc-4.0.1-standalone.jar", "hadoop-common-3.3.4.jar",
                "hadoop-auth-3.3.4.jar", "log4j-api-2.18.0.jar", "log4j-core-2.18.0.jar");
        createDriverJars("hive/4_1", "hive-jdbc-4.1.0-standalone.jar", "hadoop-common-3.4.1.jar",
                "hadoop-auth-3.4.1.jar", "log4j-api-2.18.0.jar", "log4j-core-2.18.0.jar");

        // HDP drivers
        createDriverJars("hdp/2", "hive-jdbc-1.2.1000.2.6.5.0-292-standalone.jar");

        // CDH drivers
        createDriverJars("cdh/5", "hive-jdbc-1.1.0-cdh5.16.99-standalone.jar",
                "hadoop-common-2.6.0-cdh5.16.99.jar", "hadoop-auth-2.6.0-cdh5.16.99.jar");
        createDriverJars("cdh/6", "hive-jdbc-2.1.1-cdh6.3.4-standalone.jar",
                "hadoop-common-3.0.0-cdh6.3.4.jar", "hadoop-auth-3.0.0-cdh6.3.4.jar");

        // CDP drivers
        createDriverJars("cdp/7_1_9", "hive-jdbc-3.1.3000.7.1.9.1059-4-standalone.jar",
                "hadoop-common-3.1.1.7.1.9.1059-4.jar", "hadoop-auth-3.1.1.7.1.9.1059-4.jar",
                "log4j-api-2.18.0.jar", "log4j-core-2.18.0.jar");
        createDriverJars("cdp/7_3_1", "hive-jdbc-3.1.3000.7.3.1.404-1-standalone.jar",
                "hadoop-common-3.3.5.7.3.1.404-1.jar", "hadoop-auth-3.3.5.7.3.1.404-1.jar",
                "log4j-api-2.18.0.jar", "log4j-core-2.18.0.jar");
    }

    /**
     * Helper method to create mock JAR files in a driver directory.
     */
    private void createDriverJars(String driverPath, String... jarNames) throws IOException {
        Path driverDir = tempDriverDir.resolve(driverPath);
        Files.createDirectories(driverDir);

        for (String jarName : jarNames) {
            Path jarFile = driverDir.resolve(jarName);
            Files.createFile(jarFile);
            // Write some dummy content to make it a non-empty file
            Files.writeString(jarFile, "mock jar content for " + jarName);
        }
    }

    @Test
    void testDriverBaseDirectoryInit() {
        // Verify that the temp driver directory was created
        assertThat(tempDriverDir).exists().isDirectory();
    }

    @ParameterizedTest
    @EnumSource(PlatformType.class)
    void testGetAvailableDriverTypesForEachPlatform(PlatformType platformType) {
        // Get available driver types for each platform
        List<DriverType> driverTypes = driverUtilsService.getAvailableDriverTypes(platformType);

        // Assert that we get at least one driver type for each platform
        assertThat(driverTypes)
                .as("Platform %s should have at least one available driver type", platformType)
                .isNotEmpty();

        // Log the results for visibility
        System.out.println("Platform " + platformType + " has " + driverTypes.size() + " driver type(s): " + driverTypes);

        // Verify each driver type has the expected platform in its list
        for (DriverType driverType : driverTypes) {
            assertThat(driverType.getPlatforms())
                    .as("DriverType %s should contain platform %s", driverType, platformType)
                    .contains(platformType);
        }
    }

    @ParameterizedTest
    @EnumSource(DriverType.class)
    void testBuildDriverClasspathForEachDriverType(DriverType driverType) {
        // Build classpath for each driver type
        String classpath = driverUtilsService.buildDriverClasspath(driverType);

        // Classpath should not be null (though it may be empty if no JARs exist)
        assertThat(classpath).isNotNull();

        // Log the results
        System.out.println("DriverType " + driverType + " classpath: " +
                (classpath.isEmpty() ? "(empty - no JARs found)" : classpath));

        // If we created test JARs for this driver type, verify classpath is not empty
        Path driverDir = tempDriverDir.resolve(driverType.getPath());
        if (Files.exists(driverDir)) {
            try {
                long jarCount = Files.list(driverDir)
                        .filter(path -> path.toString().endsWith(".jar"))
                        .count();

                if (jarCount > 0) {
                    assertThat(classpath)
                            .as("DriverType %s should have non-empty classpath when JARs exist", driverType)
                            .isNotEmpty();

                    // Verify the number of JAR entries matches the number of JAR files
                    String[] classpathEntries = classpath.split(File.pathSeparator);
                    assertThat(classpathEntries)
                            .as("DriverType %s should have %d JAR entries in classpath", driverType, jarCount)
                            .hasSize((int) jarCount);

                    // Verify all classpath entries are absolute paths to JAR files
                    Arrays.stream(classpathEntries).forEach(entry -> {
                        assertThat(entry)
                                .as("Classpath entry should be absolute path")
                                .startsWith(tempDriverDir.toString());
                        assertThat(entry)
                                .as("Classpath entry should point to a JAR file")
                                .endsWith(".jar");
                    });
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to list driver directory", e);
            }
        }
    }

    @Test
    void testBuildDriverClasspathWithNonExistentDirectory() {
        // Create a driver type that points to a non-existent directory
        DriverType driverType = DriverType.APACHE_HIVE_2; // Assuming this exists in enum

        // Delete the directory to simulate non-existent case
        Path driverDir = tempDriverDir.resolve(driverType.getPath());
        try {
            if (Files.exists(driverDir)) {
                Files.walk(driverDir)
                        .sorted((a, b) -> b.compareTo(a)) // Reverse order for deletion
                        .forEach(path -> {
                            try {
                                Files.delete(path);
                            } catch (IOException e) {
                                // Ignore
                            }
                        });
            }
        } catch (IOException e) {
            // Ignore
        }

        String classpath = driverUtilsService.buildDriverClasspath(driverType);

        assertThat(classpath)
                .as("Classpath should be empty for non-existent driver directory")
                .isEmpty();
    }

    @Test
    void testFromConnectionDto_WithPlatformType() {
        // Create a ConnectionDto with a platform type
        ConnectionDto connectionDto = new ConnectionDto();
        connectionDto.setHs2Uri("jdbc:hive2://localhost:10000");
        connectionDto.setPlatformType(PlatformType.CDP7_3);

        // Convert to HiveServer2Config
        HiveServer2Config config = connectionUtilService.fromConnectionDto(connectionDto);

        // Verify the URI is set
        assertThat(config.getUri()).isEqualTo("jdbc:hive2://localhost:10000");

        // Verify that jarFile is set (should include all JARs from available drivers)
        assertThat(config.getJarFile())
                .as("JarFile should be set when platform type is provided")
                .isNotNull();

        // If JARs exist for this platform, verify jarFile is not empty
        List<DriverType> driverTypes = driverUtilsService.getAvailableDriverTypes(PlatformType.CDP7_3);
        if (!driverTypes.isEmpty()) {
            boolean anyDriverHasJars = driverTypes.stream()
                    .anyMatch(dt -> {
                        Path driverDir = tempDriverDir.resolve(dt.getPath());
                        return Files.exists(driverDir);
                    });

            if (anyDriverHasJars) {
                assertThat(config.getJarFile())
                        .as("JarFile should not be empty when JARs exist for platform")
                        .isNotEmpty();
            }
        }
    }

    @Test
    void testFromConnectionDto_WithoutPlatformType() {
        // Create a ConnectionDto without a platform type
        ConnectionDto connectionDto = new ConnectionDto();
        connectionDto.setHs2Uri("jdbc:hive2://localhost:10000");

        // Convert to HiveServer2Config
        HiveServer2Config config = connectionUtilService.fromConnectionDto(connectionDto);

        // Verify the URI is set
        assertThat(config.getUri()).isEqualTo("jdbc:hive2://localhost:10000");

        // JarFile should be null or empty when no platform type is specified
        assertThat(config.getJarFile())
                .as("JarFile should be null when no platform type is provided")
                .isNull();
    }

    @Test
    void testMultipleDriversForSamePlatform() {
        // Some platforms may have multiple driver types
        // For example, HDP3 is supported by CDP_7_1_9_HIVE_DRIVER
        PlatformType platformType = PlatformType.HDP3;

        List<DriverType> driverTypes = driverUtilsService.getAvailableDriverTypes(platformType);

        assertThat(driverTypes)
                .as("HDP3 should have at least one available driver")
                .isNotEmpty();

        // Build classpath for all drivers
        for (DriverType driverType : driverTypes) {
            String classpath = driverUtilsService.buildDriverClasspath(driverType);
            assertThat(classpath).isNotNull();
            System.out.println("HDP3 - DriverType " + driverType + " classpath: " +
                    (classpath.isEmpty() ? "(empty)" : classpath));
        }
    }

    @Test
    void testDriverTypePathMapping() {
        // Verify that DriverType paths match expected structure
        assertThat(DriverType.APACHE_HIVE_2.getPath()).isEqualTo("hive/2");
        assertThat(DriverType.APACHE_HIVE_3.getPath()).isEqualTo("hive/3");
        assertThat(DriverType.APACHE_HIVE_4.getPath()).isEqualTo("hive/4");
        assertThat(DriverType.APACHE_HIVE_4_1.getPath()).isEqualTo("hive/4_1");
        assertThat(DriverType.HDP2_HIVE_DRIVER.getPath()).isEqualTo("hdp/2");
        assertThat(DriverType.CDH_5_HIVE_DRIVER.getPath()).isEqualTo("cdh/5");
        assertThat(DriverType.CDH_6_HIVE_DRIVER.getPath()).isEqualTo("cdh/6");
        assertThat(DriverType.CDP_7_1_9_HIVE_DRIVER.getPath()).isEqualTo("cdp/7_1_9");
        assertThat(DriverType.CDP_7_3_1_HIVE_DRIVER.getPath()).isEqualTo("cdp/7_3_1");
    }

    @Test
    void testDriverClassnameMapping() {
        // Verify that all DriverTypes have the correct driver class
        for (DriverType driverType : DriverType.values()) {
            assertThat(driverType.getDriverClass())
                    .as("DriverType %s should have a driver class set", driverType)
                    .isNotNull()
                    .isNotEmpty();

            // Most drivers should use the Apache Hive driver
            assertThat(driverType.getDriverClass())
                    .as("DriverType %s driver class should be valid", driverType)
                    .isIn(APACHE_HIVE_DRIVER,
                          APACHE_HIVE1_DRIVER,
                          CLOUDERA_HIVE_DRIVER,
                          CLOUDERA_HIVE1_DRIVER);
        }
    }

    @Test
    void testPlatformTypeCoverage() {
        // Verify that every PlatformType has at least one DriverType
        for (PlatformType platformType : PlatformType.values()) {
            List<DriverType> driverTypes = DriverType.findByPlatformType(platformType);
            assertThat(driverTypes)
                    .as("PlatformType %s should have at least one DriverType", platformType)
                    .isNotEmpty();
        }
    }

    @Test
    void testClasspathEntriesSeparation() throws IOException {
        // Create multiple JARs and verify they're properly separated
        String driverPath = "hive/3";
        createDriverJars(driverPath, "jar1.jar", "jar2.jar", "jar3.jar");

        String classpath = driverUtilsService.buildDriverClasspath(DriverType.APACHE_HIVE_3);

        String[] entries = classpath.split(File.pathSeparator);
        assertThat(entries)
                .as("Should have exactly 8 JAR entries (3 new + 5 from setup)")
                .hasSize(8); // 3 we just created + 5 from createTestDriverStructure

        // Verify no duplicate entries
        assertThat(entries)
                .as("Classpath should not contain duplicate entries")
                .doesNotHaveDuplicates();
    }
}
