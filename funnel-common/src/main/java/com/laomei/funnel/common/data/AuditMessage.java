package com.laomei.funnel.common.data;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author laomei on 2018/8/12 10:10
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AuditMessage {

    private String topic;

    //0 => producer client
    //1 => consumer client
    private byte part;

    //only kafka consumer client has group id;
    private String groupId = null;

    private AuditServer auditServer;

    private TimeBucket timeBucket;

    public AuditMessageDTO convertToDto() {
        AuditMessageDTO dto = new AuditMessageDTO();
        dto.setTopic(topic);
        dto.setPart(part);
        dto.setGroupId(groupId);
        dto.setIp(auditServer.getIp());
        dto.setService(auditServer.getService());
        dto.setBegin(timeBucket.getBegin());
        dto.setEnd(timeBucket.getEnd());
        dto.setMsgCount(timeBucket.getMsgCount());
        double mean = timeBucket.getStats().getMean();
        double max = timeBucket.getStats().getMax();
        double c95 = timeBucket.getStats().getPercentile(95);
        double c99 = timeBucket.getStats().getPercentile(99);
        dto.setLatencyMean(Double.isNaN(mean) ? 0 : mean);
        dto.setLatencyMax(Double.isNaN(max) ? 0 : max);
        dto.setLatencyC95(Double.isNaN(c95) ? 0 : c95);
        dto.setLatencyC99(Double.isNaN(c99) ? 0 : c99);
        return dto;
    }
}
