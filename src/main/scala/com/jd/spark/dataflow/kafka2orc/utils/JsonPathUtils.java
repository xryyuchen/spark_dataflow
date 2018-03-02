package com.jd.spark.dataflow.kafka2orc.utils;

import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.ReadContext;

import java.io.Serializable;
import java.util.List;

public class JsonPathUtils implements Serializable {

    public Double parse2Double(ReadContext ctx, String regulation, Double defaultValue) {

        try {
            Double result = ctx.read(regulation, Double.class);

            return result;
        } catch (Exception e) {

            return defaultValue;
        }
    }

    public Long parse2Long(ReadContext ctx, String regulation, Long defaultValue) {

        try {

            Long result = ctx.read(regulation, Long.class);
            return result;
        } catch (Exception e) {

            return defaultValue;
        }
    }

    public Integer parse2Int(ReadContext ctx, String regulation, Integer defaultValue) {

        try {
            Integer result = ctx.read(regulation, Integer.class);

            return result;
        } catch (Exception e) {

            return defaultValue;
        }
    }

    public String parse2String(ReadContext ctx, String regulation, String defaultValue) {

        try {
            String result = ctx.read(regulation, String.class);

            return result;
        } catch (Exception e) {

            return defaultValue;
        }
    }

    public String parse2List(ReadContext ctx, String regulation, String defaultValue) {

        try {
            List<Object> result = ctx.read(regulation, List.class);

            if (result != null) {
                return result.toString();
            }
            return defaultValue;
        } catch (Exception e) {

            return defaultValue;
        }
    }

//    public String parse2Map(ReadContext ctx, String regulation){
//
//        String result = ctx.read(regulation);
//
//        return result;
//    }

    public ReadContext parse(String json) {

        ReadContext ctx = JsonPath.parse(json);

        return ctx;
    }


}
