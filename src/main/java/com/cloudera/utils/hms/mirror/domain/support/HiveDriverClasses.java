package com.cloudera.utils.hms.mirror.domain.support;

import com.fasterxml.jackson.annotation.JsonIgnore;

public interface HiveDriverClasses {
    String APACHE_HIVE_DRIVER = "org.apache.hive.jdbc.HiveDriver";
    String APACHE_HIVE1_DRIVER = "org.apache.hadoop.hive.jdbc.HiveDriver";
    String CLOUDERA_HIVE_DRIVER = "com.cloudera.hive.jdbc.HS2Driver";
    String CLOUDERA_HIVE1_DRIVER = "com.cloudera.hive.jdbc.HS1Driver";
}
