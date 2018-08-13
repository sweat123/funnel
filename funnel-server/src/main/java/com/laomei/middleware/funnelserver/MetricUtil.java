package com.laomei.middleware.funnelserver;

import com.laomei.funnel.common.data.AuditMessageDTO;

/**
 * @author laomei on 2018/8/13 23:14
 */
public class MetricUtil {

    public static AuditMetric convertAuditMessageDtoToMetric(AuditMessageDTO auditMessage) {
        AuditMetric metric = new AuditMetric();
        metric.setTopic(auditMessage.getTopic());
        metric.setPart(auditMessage.getPart());
        metric.setGroupId(auditMessage.getGroupId());
        metric.setIp(auditMessage.getIp());
        metric.setService(auditMessage.getService());
        metric.setBegin(auditMessage.getBegin());
        metric.setEnd(auditMessage.getEnd());
        metric.setMsgCount(auditMessage.getMsgCount());
        metric.setLatencyMean(auditMessage.getLatencyMean());
        metric.setLatencyMax(auditMessage.getLatencyMax());
        metric.setLatencyC95(auditMessage.getLatencyC95());
        metric.setLatencyC99(auditMessage.getLatencyC99());
        return metric;
    }
}
