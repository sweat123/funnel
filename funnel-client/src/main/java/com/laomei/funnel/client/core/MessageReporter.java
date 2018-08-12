package com.laomei.funnel.client.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.laomei.funnel.common.data.AuditMessage;
import com.laomei.funnel.common.disruptor.RecordEntry;
import com.laomei.funnel.common.disruptor.RecordEventFactory;
import com.laomei.funnel.common.util.JsonUtil;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.laomei.funnel.client.core.ConfigConstants.AUDIT_CENTER_TOPIC;

/**
 * report audit message to audit center
 * @author laomei on 2018/8/12 12:29
 */
@Slf4j
public class MessageReporter implements AutoCloseable {

    private final KafkaProducer<String, byte[]> kafkaProducer;

    private final String auditCenterTopic;

    private final Disruptor<RecordEntry<Collection<AuditMessage>>> disruptor;

    private final RingBuffer<RecordEntry<Collection<AuditMessage>>> ringBuffer;

    private final EventTranslatorOneArg<RecordEntry<Collection<AuditMessage>>, Collection<AuditMessage>>
            translator = (event, sequence, arg0) -> event.setRecotd(arg0);

    public MessageReporter(Map<String, ?> configs) {
        val auditProducerConfigs = new HashMap<String, Object>();
        auditProducerConfigs.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                configs.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)
        );
        auditProducerConfigs.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer"
        );
        auditProducerConfigs.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArraySerializer"
        );
        this.kafkaProducer = new KafkaProducer<>(auditProducerConfigs);
        this.auditCenterTopic = String.valueOf(configs.get(AUDIT_CENTER_TOPIC));
        this.disruptor = new Disruptor<>(
                new RecordEventFactory<>(),
                16384,
                r -> {
                    val thread = new Thread(r);
                    thread.setDaemon(true);
                    thread.setName("producer-audit-service");
                    return thread;
                });
        this.disruptor.handleEventsWith(new AuditMessageEventHandler(this));
        this.ringBuffer = disruptor.getRingBuffer();
        this.disruptor.start();
    }

    /**
     * publish event to ringBuffer
     */
    void report(Collection<AuditMessage> auditMessages) {
        ringBuffer.publishEvent(translator, auditMessages);
    }

    /**
     * report audit message to audit center topic
     * @param auditMessages audit messages
     */
    private void reportToAuditCenter(Collection<AuditMessage> auditMessages) {
        for (val message : auditMessages) {
            try {
                String serializedMsg = JsonUtil.serializeAuditMessage(message.convertToDto());
                ProducerRecord<String, byte[]> record = new ProducerRecord<>(auditCenterTopic, serializedMsg.getBytes());
                kafkaProducer.send(record);
            } catch (JsonProcessingException e) {
                log.warn("serialize audit message failed; auditMessage: {}", message, e);
            }
        }
        log.info("number of {} audit messages have been sent to audit topic {}", auditMessages.size(), auditCenterTopic);
    }

    @Override
    public void close() throws Exception {
        disruptor.shutdown(30, TimeUnit.SECONDS);
        kafkaProducer.close(30, TimeUnit.SECONDS);
    }

    private static class AuditMessageEventHandler implements EventHandler<RecordEntry<Collection<AuditMessage>>> {

        private final MessageReporter messageReporter;

        AuditMessageEventHandler(MessageReporter messageReporter) {
            this.messageReporter = messageReporter;
        }

        @Override
        public void onEvent(RecordEntry<Collection<AuditMessage>> event, long sequence, boolean endOfBatch) throws Exception {
            messageReporter.reportToAuditCenter(event.getRecotd());
            event.setRecotd(null);
        }
    }
}
