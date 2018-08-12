package com.laomei.funnel.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.laomei.funnel.common.util.JsonUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * @author laomei on 2018/8/12 15:38
 */

public class JsonUtilTest {

    @Test
    public void testSerialize() throws JsonProcessingException {
        Struct struct = new Struct();
        struct.setIntValue(1);
        struct.setStrValue("struct");
        struct.setSubStruct(null);
        StringBuilder sb = new StringBuilder();
        sb.append("{")
                .append("\"strValue\":\"struct\",")
                .append("\"intValue\":1,")
                .append("\"subStruct\":null")
                .append("}");
        String json = sb.toString();
        Assert.assertEquals(json, JsonUtil.serializeAuditMessage(struct));

        SubStruct subStruct = new SubStruct("subStruct");
        struct.setSubStruct(subStruct);

        StringBuilder sb1 = new StringBuilder();
        sb1.append("{")
                .append("\"strValue\":\"struct\",")
                .append("\"intValue\":1,")
                .append("\"subStruct\":{")
                .append("\"value\":\"subStruct\"")
                .append("}")
                .append("}");
        Assert.assertEquals(sb1.toString(), JsonUtil.serializeAuditMessage(struct));
    }

    @Test
    public void testDeserialize() throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append("{")
                .append("\"strValue\":\"struct\",")
                .append("\"intValue\":1,")
                .append("\"subStruct\":{")
                .append("\"value\":\"subStruct\"")
                .append("}")
                .append("}");
        Struct struct = JsonUtil.deserializeToAuditMessage(sb.toString(), Struct.class);

        Struct structExp = new Struct();
        structExp.setIntValue(1);
        structExp.setStrValue("struct");
        SubStruct subStruct = new SubStruct("subStruct");
        structExp.setSubStruct(subStruct);

        Assert.assertEquals(structExp, struct);
    }

    @Data
    static class Struct {

        private String strValue;

        private int intValue;

        private SubStruct subStruct;
    }

    @Data
    @AllArgsConstructor
    static class SubStruct {
        private String value;
    }
}
