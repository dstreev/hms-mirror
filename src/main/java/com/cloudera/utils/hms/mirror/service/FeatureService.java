package com.cloudera.utils.hms.mirror.service;

import com.cloudera.utils.hms.mirror.domain.core.EnvironmentTable;
import com.cloudera.utils.hms.mirror.feature.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class FeatureService {

    private LegacyTranslations legacyTranslations = new LegacyTranslations();
    private BadTextFileDefFeature badTextFileDefFeature = new BadTextFileDefFeature();
    private StructEscapeFieldsFeature structEscapeFieldsFeature = new StructEscapeFieldsFeature();
//    private SparkSqlPartFeature sparkSqlPartFeature = new SparkSqlPartFeature();
    private BadRCDefFeature badRCDefFeature = new BadRCDefFeature();
    private BadParquetDefFeature badParquetDefFeature = new BadParquetDefFeature();
    private BadFieldsFFDefFeature badFieldsFFDefFeature = new BadFieldsFFDefFeature();

    public boolean fixSchema(EnvironmentTable target) {
        boolean rtn = false;

        if (legacyTranslations.fixSchema(target)) {
            log.debug("Table: {} - Feature Applicable: {}", target.getName(),  legacyTranslations.getDescription());
            target.addIssue("Feature was found applicable and adjustments applied. " +
                    legacyTranslations.getDescription());
            rtn = true;
        }
        if (badTextFileDefFeature.fixSchema(target)) {
            log.debug("Table: {} - Feature Applicable: {}", target.getName(),  badTextFileDefFeature.getDescription());
            target.addIssue("Feature was found applicable and adjustments applied. " +
                    badTextFileDefFeature.getDescription());
            rtn = true;
        }
        if (structEscapeFieldsFeature.fixSchema(target)) {
            log.debug("Table: {} - Feature Applicable: {}", target.getName(),  structEscapeFieldsFeature.getDescription());
            target.addIssue("Feature was found applicable and adjustments applied. " +
                    structEscapeFieldsFeature.getDescription());
            rtn = true;
        }
        if (badRCDefFeature.fixSchema(target)) {
            log.debug("Table: {} - Feature Applicable: {}", target.getName(),  badRCDefFeature.getDescription());
            target.addIssue("Feature was found applicable and adjustments applied. " +
                    badRCDefFeature.getDescription());
            rtn = true;
        }
        if (badParquetDefFeature.fixSchema(target)) {
            log.debug("Table: {} - Feature Applicable: {}", target.getName(),  badParquetDefFeature.getDescription());
            target.addIssue("Feature was found applicable and adjustments applied. " +
                    badParquetDefFeature.getDescription());
            rtn = true;
        }
        if (badFieldsFFDefFeature.fixSchema(target)) {
            log.debug("Table: {} - Feature Applicable: {}", target.getName(),  badFieldsFFDefFeature.getDescription());
            target.addIssue("Feature was found applicable and adjustments applied. " +
                    badFieldsFFDefFeature.getDescription());
            rtn = true;
        }
        return rtn;
    }


}
