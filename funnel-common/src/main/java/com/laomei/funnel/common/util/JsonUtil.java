package com.laomei.funnel.common.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * @author laomei on 2018/8/12 15:06
 */
public class JsonUtil {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static <T> String serializeAuditMessage(T message) throws JsonProcessingException {
        return objectMapper.writeValueAsString(message);
    }

    public static <T> T deserializeToAuditMessage(String json, Class<T> tClass) throws IOException {
        return objectMapper.readValue(json, tClass);
    }
}
