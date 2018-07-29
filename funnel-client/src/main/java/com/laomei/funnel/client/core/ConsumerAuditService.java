package com.laomei.funnel.client.core;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.HashMap;
import java.util.Map;

/**
 * @author laomei on 2018/7/28 21:35
 */
public class ConsumerAuditService<K, V> implements AutoCloseable {

    private KafkaProducer<String, byte[]> kafkaProducer;

    public ConsumerAuditService(Map<String, ?> configs) {
        Map<String, Object> auditProducerConfigs = new HashMap<>();
        auditProducerConfigs.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                configs.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)
        );
        auditProducerConfigs.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer"
        );
        auditProducerConfigs.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArraySerializer"
        );
        kafkaProducer = new KafkaProducer<>(auditProducerConfigs);
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
