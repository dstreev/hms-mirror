package com.cloudera.utils.hms.mirror.domain.dto;

import com.cloudera.utils.hms.mirror.domain.core.HybridConfig;
import com.cloudera.utils.hms.mirror.domain.support.DataStrategyEnum;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;
import lombok.Setter;

import java.util.UUID;

@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
public class JobDto {

    @Schema(description = "Unique identifier for the job", example = "3fa85f64-5717-4562-b3fc-2c963f66afa6")
    private UUID id = UUID.randomUUID();

    @Schema(description = "Unique identifier for the job", example = "job-12345")
    private String name;

    @Schema(description = "Description of the job", example = "Migration job for production tables")
    private String description;

    @Schema(description = "Creation timestamp")
    private String createdDate;

    @Schema(description = "Last modification timestamp")
    private String modifiedDate;

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

    private HybridConfig hybrid = new HybridConfig();

    @Schema(description = "Flag to indicate if this job is for a database only migration")
    private boolean databaseOnly = Boolean.FALSE;

//    private boolean replace = Boolean.FALSE;

//    @Schema(description = "Flag to indicate if this job should reset the right table")
//    private boolean resetRight = Boolean.FALSE;

    /*
    When True, this will set the 'readOnly' and 'noPurge' legacy flags where appropriate.
    TODO: WIP
     */
    @Schema(description = "Flag to indicate if this job is for disaster recovery purposes")
    private Boolean disasterRecovery = Boolean.FALSE;

    // Set by disasterRecovery.
    //    @Schema(description = "When set, the table purge option will be removed.")
//    private boolean noPurge = Boolean.FALSE;

    @Schema(description = "Flag to indicate if this job should perform a sync operation, only available if" +
            "the disasterRecovery flag is set to true")
    private Boolean sync = Boolean.FALSE;
}
