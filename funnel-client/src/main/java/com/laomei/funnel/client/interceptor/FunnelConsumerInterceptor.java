package com.laomei.funnel.client.interceptor;

import com.laomei.funnel.client.core.ConsumerAuditService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

/**
 * @author laomei on 2018/7/27 21:54
 */
@Slf4j
public class FunnelConsumerInterceptor<K, V> implements ConsumerInterceptor<K, V> {

    private ConsumerAuditService<K, V> auditService;

    public ConsumerRecords<K, V> onConsume(ConsumerRecords<K, V> records) {
        auditService.audit(records);
        return records;
    }

    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
    }

    public void close() {
        try {
            auditService.close();
        } catch (Exception e) {
            log.warn("close auditService failed!", e);
        }
    }

    public void configure(Map<String, ?> configs) {
        auditService = new ConsumerAuditService<>(configs);
    }
}
