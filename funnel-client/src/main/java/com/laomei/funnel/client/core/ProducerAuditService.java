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
import org.apache.kafka.clients.producer.ProducerRecord;

import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static com.laomei.funnel.client.core.ConfigConstants.*;

/**
 * @author laomei on 2018/7/28 21:35
 */
@Slf4j
public class ProducerAuditService<K, V> implements AutoCloseable {

    private final long timeBucketInterval;

    private AuditServer auditServer;

    private final MessageReporter messageReporter;

    private final ScheduledExecutorService singleScheduledThread;

    private final Disruptor<RecordEntry<ProducerRecord<K, V>>> disruptor;

    private final RingBuffer<RecordEntry<ProducerRecord<K, V>>> ringBuffer;

    private final EventTranslatorOneArg<RecordEntry<ProducerRecord<K, V>>, ProducerRecord<K, V>> translator = new EventTranslatorOneArg<RecordEntry<ProducerRecord<K, V>>, ProducerRecord<K, V>>() {
        @Override
        public void translateTo(RecordEntry<ProducerRecord<K, V>> event, long sequence, ProducerRecord<K, V> arg0) {
            event.setRecotd(arg0);
        }
    };

    private final ConcurrentMap<String, ProducerAuditor<K, V>> auditorForTopic;

    public ProducerAuditService(Map<String, ?> configs) {
        this.messageReporter = new MessageReporter(configs);
        this.timeBucketInterval = Long.valueOf(String.valueOf(configs.get(TIME_BUCKET_INTERVAL)));
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
            Thread thread = new Thread(r, "funnel-producer-trigger-thread");
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
        this.disruptor.handleEventsWith(new ProducerRecordEventHandler<>(this));
        this.disruptor.start();
        this.auditorForTopic = new ConcurrentHashMap<>();
        startSendTriggerRecord();
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
        messageReporter.close();
        singleScheduledThread.shutdownNow();
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
        val auditMessages = timeBuckets.stream()
                .map(timeBucket -> new AuditMessage(topic, (byte) 0, null, auditServer, timeBucket))
                .collect(Collectors.toList());
        messageReporter.report(auditMessages);
    }

    private void startSendTriggerRecord() {
        singleScheduledThread.scheduleWithFixedDelay(this::sendTriggerRecord, timeBucketInterval, timeBucketInterval, TimeUnit.MILLISECONDS);
    }

    private void sendTriggerRecord() {
        for (String topic : auditorForTopic.keySet()) {
            audit(new ProducerRecord<>(topic, null, 0L, null, null, null));
        }
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
