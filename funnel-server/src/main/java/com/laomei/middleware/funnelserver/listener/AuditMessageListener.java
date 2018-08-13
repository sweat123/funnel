package com.laomei.middleware.funnelserver.listener;

import com.laomei.middleware.funnelserver.config.FunnelServerConfig;
import com.laomei.middleware.funnelserver.service.AuditService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author laomei on 2018/8/13 21:38
 */
@Component
public class AuditMessageListener {

    @Autowired
    private FunnelServerConfig funnelServerConfig;

    @Autowired
    private AuditService auditService;

    @KafkaListener(topics = "#{funnelServerConfig.getAuditCenterTopic()}")
    public void listen(List<ConsumerRecord<String, byte[]>> records) {
        auditService.auditRecords(records);
    }
}
