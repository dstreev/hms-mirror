package com.cloudera.utils.hms.mirror.domain.support;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
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
    private boolean dryRun = Boolean.TRUE;

    public boolean isExecute() {
        return !dryRun;
    }

}
