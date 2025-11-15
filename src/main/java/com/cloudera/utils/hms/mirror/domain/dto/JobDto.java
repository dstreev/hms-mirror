package com.cloudera.utils.hms.mirror.domain.dto;

import com.cloudera.utils.hms.mirror.domain.core.HybridConfig;
import com.cloudera.utils.hms.mirror.domain.support.DataStrategyEnum;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

import static org.apache.commons.lang3.StringUtils.isBlank;

@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
public class JobDto implements Cloneable {

    private static final DateTimeFormatter KEY_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmssSSS");

    // This would be the top level Key for the RocksDB columnFamily.
    @Schema(description = "Primary key used for RocksDB storage. Automatically generated with timestamp and UUID suffix. " +
            "This serves as the unique identifier in the RocksDB column family for job persistence",
            accessMode = Schema.AccessMode.READ_ONLY,
            example = "20250115_143045123_a7f9")
    private String key = LocalDateTime.now().format(KEY_FORMATTER) + "_" + UUID.randomUUID().toString().substring(0, 4);

    @Schema(description = "Name for the job", example = "job-12345")
    private String name;

    @Schema(description = "Description of the job", example = "Migration job for production tables")
    private String description;

    @Schema(description = "Timestamp when this job was first created",
            accessMode = Schema.AccessMode.READ_ONLY)
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private LocalDateTime created;

    @Schema(description = "Timestamp when this job was last modified",
            accessMode = Schema.AccessMode.READ_ONLY)
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private LocalDateTime modified;

    @Schema(description = "A reference to the dataset used for this job")
    private String datasetReference;
    @Schema(description = "A reference to the configuration used for this job")
    private String configReference;

    @Schema(description = "A reference to the left connection used for this job")
    private String leftConnectionReference;
    @Schema(description = "A reference to the right connection used for this job")
    private String rightConnectionReference;

    @Schema(description = "The data strategy for this job")
    private DataStrategyEnum strategy;

    @Schema(description = "Optional intermediate storage location for data transfer operations")
    private String intermediateStorage;

    @Schema(description = "Optional target namespace for the migration")
    private String targetNamespace;

    @Schema(description = "Hybrid migration configuration combining SQL and EXPORT_IMPORT strategies. " +
            "Used when strategy is set to HYBRID to control which approach is used for different table types",
            implementation = HybridConfig.class)
    private HybridConfig hybrid = new HybridConfig();

    @Schema(description = "Flag to indicate if this job is for a database only migration")
    private boolean databaseOnly = Boolean.FALSE;

    @Schema(description = "Flag to indicate if this job should consolidate DB create statements")
    private boolean consolidateDBCreateStatements = Boolean.FALSE;

    @Schema(description = "Path to test data file for loading test data instead of running actual migration. " +
            "Used for testing and development purposes. When set, the job loads pre-recorded metadata instead of connecting to clusters",
            example = "/path/to/test-data.yaml")
    private String loadTestDataFile = null;

    @Schema(description = "Skip validation of HDFS/S3 links and locations. " +
            "When true, location validation is bypassed which can speed up processing but may miss configuration issues",
            defaultValue = "false",
            example = "false")
    private boolean skipLinkCheck = Boolean.FALSE;

    /*
        When True, this will set the 'readOnly' and 'noPurge' legacy flags where appropriate.
        TODO: WIP
         */
    @Schema(description = "Flag to indicate if this job is for disaster recovery purposes")
    private boolean disasterRecovery = Boolean.FALSE;

    // Set by disasterRecovery.

    @Schema(description = "Flag to indicate if this job should perform a sync operation, only available if" +
            "the disasterRecovery flag is set to true")
    private boolean sync = Boolean.FALSE;

    public boolean isReadOnly() {
        return disasterRecovery;
    }

    public boolean isNoPurge() {
        return disasterRecovery;
    }

    @JsonIgnore
    public boolean isLoadingTestData() {
        if (!isBlank(loadTestDataFile)) {
            return Boolean.TRUE;
        } else {
            return Boolean.FALSE;
        }
    }

    /**
     * Create a deep clone of this JobDto.
     * All nested objects are cloned to avoid shared references.
     *
     * @return A deep clone of this JobDto
     */
    public JobDto clone() {
        JobDto clone = null;
        try {
            clone = (JobDto) super.clone();

            // Copy immutable and primitive fields
//            clone.id = (this.id != null) ? UUID.fromString(this.id.toString()) : UUID.randomUUID();
//            clone.name = this.name;
//            clone.description = this.description;
//            clone.createdDate = this.createdDate;
//            clone.modifiedDate = this.modifiedDate;
//            clone.datasetReference = this.datasetReference;
//            clone.configReference = this.configReference;
//            clone.leftConnectionReference = this.leftConnectionReference;
//            clone.rightConnectionReference = this.rightConnectionReference;
//            clone.strategy = this.strategy; // enum is immutable
//            clone.databaseOnly = this.databaseOnly;
//            clone.disasterRecovery = this.disasterRecovery;
//            clone.sync = this.sync;
//            clone.loadTestDataFile = this.loadTestDataFile;
//            clone.skipLinkCheck = this.skipLinkCheck;

            // Deep clone hybrid config
            if (this.hybrid != null) {
                clone.hybrid = this.hybrid.clone();
            } else {
                clone.hybrid = new HybridConfig();
            }
        } catch (CloneNotSupportedException e) {
            throw new AssertionError("Clone not supported", e);
        }

        return clone;
    }
}
