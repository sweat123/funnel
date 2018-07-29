package com.laomei.funnel.client.interceptor;

import com.laomei.funnel.client.core.ProducerAuditService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author laomei on 2018/7/27 21:58
 */
@Slf4j
public class FunnelProducerInterceptor<K, V> implements ProducerInterceptor<K, V> {

    private ProducerAuditService<K, V> auditService;

    public ProducerRecord<K, V> onSend(ProducerRecord<K, V> record) {
        auditService.audit(record);
        return record;
    }

    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
    }

    public void close() {
        try {
            auditService.close();
        } catch (Exception e) {
            log.warn("close auditService failed!", e);
        }
    }

    public void configure(Map<String, ?> configs) {
        auditService = new ProducerAuditService<>(configs);
    }
}
