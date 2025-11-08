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

package com.cloudera.utils.hms.mirror.service;

import com.cloudera.utils.hms.mirror.domain.dto.ConnectionDto;
import com.cloudera.utils.hms.mirror.domain.support.DriverType;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.domain.support.PlatformType;
import com.cloudera.utils.hms.util.DriverShim;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.commons.lang3.StringUtils.isBlank;

@Component
@Slf4j
@RequiredArgsConstructor
public class DriverUtilsService {
    /**
     * Base directory for driver library files.
     * Default: ${user.home}/.hms-mirror/drivers
     * Can be overridden via property: hms-mirror.drivers.base-dir
     */
    @Value("${hms-mirror.drivers.base-dir:${user.home}/.hms-mirror/drivers}")
    private String driverBaseDirectory;

    /**
     * Set the driver base directory. Primarily used for testing.
     * @param driverBaseDirectory The base directory path
     */
    public void setDriverBaseDirectory(String driverBaseDirectory) {
        this.driverBaseDirectory = driverBaseDirectory;
    }

    @PostConstruct
    public void init() {
        log.info("Driver library base directory configured: {}", driverBaseDirectory);

        // Validate and create directory if it doesn't exist
        try {
            Path driverPath = Paths.get(driverBaseDirectory);
            if (!Files.exists(driverPath)) {
                log.info("Driver base directory does not exist. Creating: {}", driverBaseDirectory);
                Files.createDirectories(driverPath);
                log.info("Driver base directory created successfully");
            } else {
                log.debug("Driver base directory already exists: {}", driverBaseDirectory);
            }
        } catch (Exception e) {
            log.error("Failed to create driver base directory: {}", driverBaseDirectory, e);
        }
    }

    /**
     * Get list of available DriverTypes for a given PlatformType.
     *
     * @param platformType The platform type
     * @return List of DriverTypes that support this platform
     */
    public List<DriverType> getAvailableDriverTypes(PlatformType platformType) {
        log.debug("Finding available driver types for platform: {}", platformType);
        List<DriverType> driverTypes = DriverType.findByPlatformType(platformType);
        log.info("Found {} driver type(s) available for platform {}: {}",
                driverTypes.size(), platformType, driverTypes);
        return driverTypes;
    }

    /**
     * Builds a classpath string of all JAR files for a specific DriverType.
     * Scans the driver-specific directory under the base driver directory.
     *
     * @param driverType The DriverType to build classpath for
     * @return Classpath string with all JAR files separated by path separator, or empty string if none found
     */
    public String buildDriverClasspath(DriverType driverType) {
        log.debug("Building driver classpath for driver type: {}", driverType);

        String driverPath = driverType.getPath();
        Path driverDirectory = Paths.get(driverBaseDirectory, driverPath);

        log.debug("Scanning driver directory: {}", driverDirectory);

        if (!Files.exists(driverDirectory)) {
            log.warn("Driver directory does not exist: {}", driverDirectory);
            return "";
        }

        if (!Files.isDirectory(driverDirectory)) {
            log.warn("Driver path is not a directory: {}", driverDirectory);
            return "";
        }

        // Find all JAR files in this driver directory
        try (Stream<Path> files = Files.list(driverDirectory)) {
            List<String> jarPaths = files
                    .filter(Files::isRegularFile)
                    .filter(path -> path.toString().toLowerCase().endsWith(".jar"))
                    .map(Path::toAbsolutePath)
                    .map(Path::toString)
                    .toList();

            if (jarPaths.isEmpty()) {
                log.warn("No JAR files found in directory: {}", driverDirectory);
                return "";
            }

            // Build classpath string
            String classpath = String.join(File.pathSeparator, jarPaths);
            log.info("Built driver classpath for {}: {} JAR(s) found in {}",
                    driverType, jarPaths.size(), driverDirectory);
            log.debug("Full classpath: {}", classpath);

            return classpath;

        } catch (IOException e) {
            log.error("Error reading driver directory: {}", driverDirectory, e);
            return "";
        }
    }

    // This is a shim process that allows us to load a Hive Driver from
    // a jar File, via a new ClassLoader.
    @SuppressWarnings("unchecked")
    public Driver getHs2Driver(ConnectionDto connection, Environment environment) {
        Driver hiveShim = null;
        try {
            String jarFile = buildDriverClasspath(connection.getHs2DriverType());
            if (!isBlank(jarFile)) {
                String[] files = jarFile.split(":");
                URL[] urls = new URL[files.length];
                File[] jarFiles = new File[files.length];
                for (int i = 0; i < files.length; i++) {
                    jarFiles[i] = new File(files[i]);
                    if (!jarFiles[i].exists()) {
                        log.error("Jarfile: " + files[i] + " can't be located.");
                        throw new AssertionError("Jarfile: " + files[i] + " can't be located.");
                    }
                    urls[i] = jarFiles[i].toURI().toURL();
                }

                log.trace("Building Classloader to isolate JDBC Library for: {}", jarFile);
                URLClassLoader hive3ClassLoader = URLClassLoader.newInstance(urls, connection.getClass().getClassLoader());
                log.trace("Loading Hive JDBC Driver");
                Class<?> classToLoad = hive3ClassLoader.loadClass(connection.getHs2DriverType().getDriverClass());
                Package aPackage = classToLoad.getPackage();
                String implementationVersion = aPackage.getImplementationVersion();
                connection.setHs2Version(implementationVersion);
                log.info("{} - Hive JDBC Implementation Version: {}", environment, implementationVersion);
                Driver hiveDriver = (Driver) classToLoad.getDeclaredConstructor().newInstance();
                log.trace("Building Hive Driver Shim");
                hiveShim = new DriverShim(hiveDriver);
                log.trace("Registering Hive Shim Driver with JDBC 'DriverManager'");
            /*  I think this is no longer needed since we are now controlling all the drivers.
            } else {
                Class<?> hiveDriverClass = Class.forName(hs2Config.getDriverClassName());
                hiveShim = (Driver) hiveDriverClass.getDeclaredConstructor().newInstance();
                Package aPackage = hiveDriverClass.getPackage();
                String implementationVersion = aPackage.getImplementationVersion();
                hs2Config.setVersion(implementationVersion);
                log.info("{} - Hive JDBC Implementation Version: {}", environment, implementationVersion);

             */
            }
            DriverManager.registerDriver(hiveShim);
        } catch (SQLException | MalformedURLException |
                 ClassNotFoundException | InstantiationException |
                 IllegalAccessException throwables) {
            log.error(throwables.getMessage(), throwables);
        } catch (InvocationTargetException | NoSuchMethodException e) {
            log.error("Issue getting Driver", e);
//            throw new RuntimeException(e);
        }
        return hiveShim;
    }

    public static void deregisterDriver(Driver hiveShim) {
        try {
            log.trace("De-registering Driver from 'DriverManager'");
            DriverManager.deregisterDriver(hiveShim);
        } catch (SQLException throwables) {
            log.error(throwables.getMessage(), throwables);
        }
    }

    //    public class JarFilePathResolver {
    public static String byGetProtectionDomain(Class clazz) throws URISyntaxException {
        URL url = clazz.getProtectionDomain().getCodeSource().getLocation();
        return Paths.get(url.toURI()).toString();
    }
//    }

}
