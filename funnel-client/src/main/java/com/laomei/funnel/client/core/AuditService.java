package com.laomei.funnel.client.core;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Map;

/**
 * @author laomei on 2018/7/27 22:07
 */
@Slf4j
public class AuditService<K, V> implements AutoCloseable {

    public AuditService(Map<String, ?> configs) {

    }

    /**
     * audit ProducerRecord
     * @param record ProducerRecord
     */
    public void audit(ProducerRecord<K, V> record) {

    }

    /**
     * audit ConsumerRecords
     * @param records ConsumerRecords
     */
    public void audit(ConsumerRecords<K, V> records) {

    }

    @Override
    public void close() throws Exception {

    }
}
