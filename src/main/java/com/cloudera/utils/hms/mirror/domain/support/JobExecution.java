package com.cloudera.utils.hms.mirror.domain.support;


import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/*
Keep track of things realted to the jobs execution.
 */
@Slf4j
@Getter
@Setter
public class JobExecution {
    private boolean dryRun = Boolean.TRUE;

    public boolean isExecute() {
        return !dryRun;
    }

}
