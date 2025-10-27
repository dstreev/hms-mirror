package com.cloudera.utils.hms.mirror.service;

import com.cloudera.utils.hms.mirror.domain.core.HiveServer2Config;
import com.cloudera.utils.hms.mirror.domain.dto.ConnectionDto;
import com.cloudera.utils.hms.mirror.domain.support.DriverType;
import com.cloudera.utils.hms.mirror.domain.support.PlatformType;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Service for connection utilities including driver library management.
 */
@Component
@Slf4j
@Getter
@RequiredArgsConstructor
public class ConnectionUtilService {

    @NonNull
    private final DriverUtilsService driverUtilsService;

    /**
     * Builds a HiveServer2Config from a ConnectionDto, including driver JAR classpath
     * based on the platform type.
     *
     * @param connectionDto The connection configuration
     * @return HiveServer2Config with URI and driver JARs configured
     */
    public HiveServer2Config fromConnectionDto(ConnectionDto connectionDto) {
        HiveServer2Config hiveServer2Config = new HiveServer2Config();
        hiveServer2Config.setUri(connectionDto.getHs2Uri());

        // Build the driver JAR classpath based on platform type
        if (connectionDto.getPlatformType() != null) {
            // Get all available driver types for this platform
            List<DriverType> availableDrivers = driverUtilsService.getAvailableDriverTypes(connectionDto.getPlatformType());

            if (!availableDrivers.isEmpty()) {
                // For now, collect JARs from all available drivers
                // TODO: Allow user to specify which driver to use
                List<String> allJarPaths = new ArrayList<>();
                for (DriverType driverType : availableDrivers) {
                    String driverClasspath = driverUtilsService.buildDriverClasspath(driverType);
                    if (!driverClasspath.isEmpty()) {
                        allJarPaths.addAll(List.of(driverClasspath.split(File.pathSeparator)));
                    }
                }

                if (!allJarPaths.isEmpty()) {
                    String jarFilesClasspath = String.join(File.pathSeparator, allJarPaths);
                    hiveServer2Config.setJarFile(jarFilesClasspath);
                    log.debug("Set jarFile classpath for platform {}: {} JAR(s) from {} driver type(s)",
                        connectionDto.getPlatformType(), allJarPaths.size(), availableDrivers.size());
                } else {
                    log.warn("No driver JAR files found for platform type: {}",
                        connectionDto.getPlatformType());
                }
            } else {
                log.warn("No driver types available for platform: {}", connectionDto.getPlatformType());
            }
        } else {
            log.warn("Platform type not specified in ConnectionDto - driver JARs not configured");
        }

        hiveServer2Config.getConnectionProperties().putAll(connectionDto.getHs2ConnectionProperties());

        return hiveServer2Config;
    }

}
