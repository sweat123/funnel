package com.laomei.funnel.client.core;

import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * @author laomei on 2018/7/29 15:57
 */
public class ProducerAuditor<K, V> {

    private final long timeBucketInterval;

    public ProducerAuditor(long timeBucketInterval) {
        this.timeBucketInterval = timeBucketInterval;
    }

    public void audit(ProducerRecord<K, V> record) {
    }
}
