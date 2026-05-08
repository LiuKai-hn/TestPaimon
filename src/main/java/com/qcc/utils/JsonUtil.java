package com.qcc.utils;


/*
 *
 * @author liukai
 * @date 2026/5/8 16:37
 */

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public class JsonUtil {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static String toJson(Object obj) {
        try {
            return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(obj);
        } catch (Exception e) {
            return "{}";
        }
    }
}