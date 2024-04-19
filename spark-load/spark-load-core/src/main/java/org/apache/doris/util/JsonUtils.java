package org.apache.doris.util;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;

import java.io.File;
import java.io.IOException;

/**
 * json utilities
 */
public class JsonUtils {

    private static final ObjectMapper MAPPER =
            JsonMapper.builder().enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS).build();

    public static <T> T readValue(String s, Class<T> clazz) throws JsonProcessingException {
        return MAPPER.readValue(s, clazz);
    }

    public static <T> T readValue(String s, TypeReference<T> ref) throws JsonProcessingException {
        return MAPPER.readValue(s, ref);
    }

    public static <T> T readValue(File file, Class<T> clazz) throws IOException {
        return MAPPER.readValue(file, clazz);
    }

    public static <T> T readValue(JsonParser parser, Class<T> clazz) throws IOException {
        return MAPPER.readValue(parser, clazz);
    }

    public static <T> T readValue(JsonParser parser, TypeReference<T> ref) throws IOException {
        return MAPPER.readValue(parser, ref);
    }

    public static String writeValueAsString(Object o) throws JsonProcessingException {
        return MAPPER.writeValueAsString(o);
    }

    public static byte[] writeValueAsBytes(Object o) throws JsonProcessingException {
        return MAPPER.writeValueAsBytes(o);
    }

}
