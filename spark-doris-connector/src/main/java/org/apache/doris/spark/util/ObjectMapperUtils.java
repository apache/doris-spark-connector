package org.apache.doris.spark.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;


public class ObjectMapperUtils {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static String writeValueAsString(Object value) throws JsonProcessingException {
        return objectMapper.writeValueAsString(value);
    }


}
