package com.streever.hadoop.hms.mirror.feature;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class FeaturesCombinedTest {

    @Test
    public void test_parquet_001() {
        List<String> schema = toList(BadParquetDefFeatureTest.schema_01);
        FeaturesEnum check = doit(schema);
        assertEquals(check, FeaturesEnum.BAD_PARQUET_DEF);
        schema.stream().forEach(System.out::println);
    }

    @Test
    public void test_parquet_002() {
        List<String> schema = toList(BadParquetDefFeatureTest.schema_02);
        FeaturesEnum check = doit(schema);
        assertEquals(check, FeaturesEnum.BAD_PARQUET_DEF);
        schema.stream().forEach(System.out::println);
    }

    @Test
    public void test_orc_003() {
        List<String> schema = toList(BadOrcDefFeatureTest.schema_01);
        FeaturesEnum check = doit(schema);
        assertEquals(check, FeaturesEnum.BAD_ORC_DEF);
        schema.stream().forEach(System.out::println);
    }

    @Test
    public void test_orc_004() {
        List<String> schema = toList(BadOrcDefFeatureTest.schema_02);
        FeaturesEnum check = doit(schema);
        assertEquals(check, null);
        schema.stream().forEach(System.out::println);
    }

    @Test
    public void test_rc_005() {
        List<String> schema = toList(BadRCDefFeatureTest.schema_01);
        FeaturesEnum check = doit(schema);
        assertEquals(check, FeaturesEnum.BAD_RC_DEF);
        schema.stream().forEach(System.out::println);
    }

//    @Test
//    public void test_006() {
//        List<String> resultList = doit(BadRCDefFeatureTest.schema_02);
//        resultList.stream().forEach(System.out::println);
//    }

    @Test
    public void test_textfile_007() {
        List<String> schema = toList(BadTextfileDefFeatureTest.schema_01);
        FeaturesEnum check = doit(schema);
        assertEquals(check, FeaturesEnum.BAD_TEXTFILE_DEF);
        schema.stream().forEach(System.out::println);
    }

    @Test
    public void test_textfile_008() {
        List<String> schema = toList(BadTextfileDefFeatureTest.schema_02);
        FeaturesEnum check = doit(schema);
        assertEquals(check, FeaturesEnum.BAD_TEXTFILE_DEF);
        schema.stream().forEach(System.out::println);
    }

    public FeaturesEnum doit(List<String> schema) {
        FeaturesEnum appliedFeature = null;
        for (FeaturesEnum features : FeaturesEnum.values()) {
            Feature feature = features.getFeature();
            if (feature.applicable(schema)) {
                System.out.println("Adjusting: " + feature.getDescription());
                if (feature.fixSchema(schema)) {
                    if (appliedFeature != null) {
                        assertFalse("Feature: " + appliedFeature.toString() + " was already applied." +
                                "Now attempting to applied second feature: " + features.toString(), Boolean.TRUE);
                    }
                    appliedFeature = features;
                }
            }
        }
        return appliedFeature;
    }

    private List<String> toList(String[] array) {
        List<String> rtn = new ArrayList<String>();
        Collections.addAll(rtn, array);
        return rtn;
    }

}
