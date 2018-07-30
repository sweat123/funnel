package com.laomei.funnel.client.core;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import lombok.val;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static com.laomei.funnel.client.common.ConfigConstants.TIME_BUCKET_INTERVAL;

/**
 * @author laomei on 2018/7/28 21:35
 */
public class ProducerAuditService<K, V> implements AutoCloseable {

    private long timeBucketInterval;

    private final Disruptor<RecordEntry<ProducerRecord<K, V>>> disruptor;

    private final KafkaProducer<String, byte[]> kafkaProducer;

    private final RingBuffer<RecordEntry<ProducerRecord<K, V>>> ringBuffer;

    private final EventTranslatorOneArg<RecordEntry<ProducerRecord<K, V>>, ProducerRecord<K, V>> translator = new EventTranslatorOneArg<RecordEntry<ProducerRecord<K, V>>, ProducerRecord<K, V>>() {
        @Override
        public void translateTo(RecordEntry<ProducerRecord<K, V>> event, long sequence, ProducerRecord<K, V> arg0) {
            event.setRecotd(arg0);
        }
    };

    private final ConcurrentMap<String, ProducerAuditor<K, V>> auditorForTopic;

    public ProducerAuditService(Map<String, ?> configs) {
        this.timeBucketInterval = Long.valueOf(String.valueOf(configs.get(TIME_BUCKET_INTERVAL)));
        val auditProducerConfigs = new HashMap<String, Object>();
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
        this.kafkaProducer = new KafkaProducer<>(auditProducerConfigs);
        this.disruptor = new Disruptor<>(
                new RecordEventFactory<ProducerRecord<K, V>>(),
                16384,
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        val thread = new Thread(r);
                        thread.setDaemon(true);
                        thread.setName("producer-audit-service");
                        return thread;
                    }
                });
        this.ringBuffer = disruptor.getRingBuffer();
        this.disruptor.handleEventsWith(new ProducerRecordEventHandler<>(this));
        this.disruptor.start();
        this.auditorForTopic = new ConcurrentHashMap<>();
    }

    /**
     * publish event in disruptor
     * @param record ProducerRecord
     */
    public void audit(ProducerRecord<K, V> record) {
        ringBuffer.publishEvent(translator, record);
    }

    @Override
    public void close() throws Exception {
        disruptor.shutdown(30, TimeUnit.SECONDS);
    }

    /**
     * audit ProducerRecord
     * @param record ProducerRecord
     */
    private void auditRecord(ProducerRecord<K, V> record) {
        String topic = record.topic();
        ProducerAuditor<K, V> auditor = auditorForTopic.get(topic);
        if (auditor == null) {
            auditor = new ProducerAuditor<>(timeBucketInterval);
            auditorForTopic.putIfAbsent(topic, auditor);
        }
        boolean needReport = auditor.audit(record);
        if (needReport) {
            val waitForReportTimeBuckets = auditor.getAndResetWaitReportedTimeBuckets();
            if (!waitForReportTimeBuckets.isEmpty()) {
                report(topic, waitForReportTimeBuckets);
            }
        }
    }

    /**
     * report timeBuckets to audit topic
     * @param topic topic for record
     * @param timeBuckets time buckets
     */
    private void report(String topic, Set<TimeBucket> timeBuckets) {
        //TODO: report buckets
    }

    private static class ProducerRecordEventHandler<K, V> implements EventHandler<RecordEntry<ProducerRecord<K, V>>> {

        private final ProducerAuditService<K, V> auditService;

        ProducerRecordEventHandler(ProducerAuditService<K, V> auditService) {
            this.auditService = auditService;
        }

        @Override
        public void onEvent(RecordEntry<ProducerRecord<K, V>> producerRecordRecordEntry, long l, boolean b) throws Exception {
            auditService.auditRecord(producerRecordRecordEntry.getRecotd());
            producerRecordRecordEntry.setRecotd(null);
        }
    }
}
