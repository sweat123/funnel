package com.laomei.funnel.client.core;

import com.laomei.funnel.common.data.AuditMessage;
import com.laomei.funnel.common.data.AuditServer;
import com.laomei.funnel.common.data.TimeBucket;
import com.laomei.funnel.common.disruptor.RecordEntry;
import com.laomei.funnel.common.disruptor.RecordEventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static com.laomei.funnel.client.core.FunnelConfigs.*;

/**
 * @author laomei on 2018/7/28 21:35
 */
@Slf4j
public class ConsumerAuditService<K, V> implements AutoCloseable {

    private final long timeBucketInterval;

    private final String groupId;

    private final AuditServer auditServer;

    private final ConcurrentMap<String, ConsumerAuditor<K, V>> auditorForTopic;

    private final MessageReporter messageReporter;

    private final ScheduledExecutorService singleScheduledThread;

    private final Disruptor<RecordEntry<ConsumerRecords<K, V>>> disruptor;

    private final RingBuffer<RecordEntry<ConsumerRecords<K, V>>> ringBuffer;

    private final EventTranslatorOneArg<RecordEntry<ConsumerRecords<K, V>>, ConsumerRecords<K, V>> translator = new EventTranslatorOneArg<RecordEntry<ConsumerRecords<K, V>>, ConsumerRecords<K, V>>() {
        @Override
        public void translateTo(RecordEntry<ConsumerRecords<K, V>> event, long sequence, ConsumerRecords<K, V> arg0) {
            event.setRecotd(arg0);
        }
    };

    public ConsumerAuditService(Map<String, ?> configs) {
        this.messageReporter = new MessageReporter(configs);
        this.groupId = String.valueOf(configs.get(AUDIT_CLIENT_GROUP_ID));
        this.timeBucketInterval = Long.valueOf(String.valueOf(configs.get(TIME_BUCKET_INTERVAL_MS)));
        String service = String.valueOf(configs.get(AUDIT_CLIENT_SERVICE));
        String ip = String.valueOf(configs.get(AUDIT_CLIENT_IP));
        if (Objects.isNull(ip) || "".equals(ip)) {
            try {
                ip = Inet4Address.getLocalHost().getHostAddress();
            } catch (UnknownHostException e) {
                log.warn("failed to get ip for producerInterceptor, default value will be set;");
                ip = "unknown-ip";
            }
        }
        Objects.requireNonNull(service, "funnel.audit.client.service must be set for funnel interceptor;");
        this.auditServer = new AuditServer(service, ip);
        this.singleScheduledThread = Executors.newScheduledThreadPool(1, r -> {
            Thread thread = new Thread(r, "funnel-consumer-trigger-thread");
            thread.setDaemon(true);
            return thread;
        });
        this.disruptor = new Disruptor<>(
                new RecordEventFactory<>(),
                16384,
                r -> {
                    val thread = new Thread(r);
                    thread.setDaemon(true);
                    thread.setName("producer-audit-service");
                    return thread;
                });
        this.ringBuffer = disruptor.getRingBuffer();
        this.disruptor.handleEventsWith(new ConsumerRecordEventHandler<>(this));
        this.disruptor.start();
        this.auditorForTopic = new ConcurrentHashMap<>();
        startSendTriggerRecord();
    }

    /**
     * audit ConsumerRecords
     * @param records ConsumerRecords
     */
    public void audit(ConsumerRecords<K, V> records) {
        ringBuffer.publishEvent(translator, records);
    }

    @Override
    public void close() throws Exception {
        messageReporter.close();
        disruptor.shutdown(30, TimeUnit.SECONDS);
        singleScheduledThread.shutdown();
    }

    private void auditRecords(ConsumerRecords<K, V> records) {
        records.forEach(this::auditRecord);
    }

    private void auditRecord(ConsumerRecord<K, V> record) {
        String topic = record.topic();
        ConsumerAuditor<K, V> auditor = auditorForTopic.get(topic);
        if (auditor == null) {
            auditor = new ConsumerAuditor<>(timeBucketInterval);
            auditorForTopic.put(topic, auditor);
        }
        boolean needReport = auditor.audit(record);
        if (needReport) {
            Set<TimeBucket> waitForReportTimeBuckets = auditor.getAndResetWaitReportedTimeBuckets();
            if (!waitForReportTimeBuckets.isEmpty()) {
                report(topic, waitForReportTimeBuckets);
            }
        }
    }

    private void report(String topic, Set<TimeBucket> timeBuckets) {
        val auditMessages = timeBuckets.stream()
                .map(timeBucket -> new AuditMessage(topic, (byte) 1, groupId, auditServer, timeBucket))
                .collect(Collectors.toList());
        messageReporter.report(auditMessages);
    }

    private void startSendTriggerRecord() {
        singleScheduledThread.scheduleWithFixedDelay(this::sendTriggerRecord, timeBucketInterval, timeBucketInterval, TimeUnit.MILLISECONDS);
    }

    private void sendTriggerRecord() {
        val triggerRecords = new HashMap<TopicPartition, List<ConsumerRecord<K, V>>>(auditorForTopic.size());
        for (String topic : auditorForTopic.keySet()) {
            ConsumerRecord<K, V> record = new ConsumerRecord<>(topic, -1, -1, null, null);
            triggerRecords.put(new TopicPartition(topic, -1), Collections.singletonList(record));
        }
        audit(new ConsumerRecords<>(triggerRecords));
    }

    private static class ConsumerRecordEventHandler<K, V> implements EventHandler<RecordEntry<ConsumerRecords<K, V>>> {

        private final ConsumerAuditService<K, V> auditService;

        ConsumerRecordEventHandler(ConsumerAuditService<K, V> auditService) {
            this.auditService = auditService;
        }

        @Override
        public void onEvent(RecordEntry<ConsumerRecords<K, V>> consumerRecordsRecordEntry, long l, boolean b) throws Exception {
            auditService.auditRecords(consumerRecordsRecordEntry.getRecotd());
            consumerRecordsRecordEntry.setRecotd(null);
        }
    }
}
