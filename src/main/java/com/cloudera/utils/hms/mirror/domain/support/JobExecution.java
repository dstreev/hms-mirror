package com.cloudera.utils.hms.mirror.domain.support;


import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.util.Date;

/*
Keep track of things realted to the jobs execution.
 */
@Slf4j
@Getter
@Setter
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class JobExecution {
    @Schema(description = "Flag to indicate if this is a dry run execution. " +
            "When true, the job validates and plans the migration without making any changes to the target cluster. " +
            "When false (execute mode), the job performs actual data and metadata migrations",
            defaultValue = "true",
            example = "false")
    private boolean dryRun = Boolean.TRUE;

    @JsonIgnore
    public boolean isExecute() {
        return !dryRun;
    }

}
