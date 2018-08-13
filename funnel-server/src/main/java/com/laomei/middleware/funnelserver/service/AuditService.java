package com.laomei.middleware.funnelserver.service;

import com.laomei.funnel.common.data.AuditMessageDTO;
import com.laomei.funnel.common.util.JsonUtil;
import com.laomei.middleware.funnelserver.AuditMetric;
import com.laomei.middleware.funnelserver.MetricUtil;
import com.laomei.middleware.funnelserver.dao.AuditMetricRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * @author laomei on 2018/8/13 21:56
 */
@Slf4j
@Service
public class AuditService {

    private final AuditMetricRepository auditMetricRepository;

    private final AtomicLong saveDocumentNum;

    public AuditService(final AuditMetricRepository auditMetricRepository) {
        this.auditMetricRepository = auditMetricRepository;
        this.saveDocumentNum = new AtomicLong(0);
    }

    public void auditRecords(List<ConsumerRecord<String, byte[]>> records) {
        List<AuditMetric> auditMetrics = records.stream()
                .map(ConsumerRecord::value)
                .map(value -> {
                    try {
                        return JsonUtil.deserializeToAuditMessage(new String(value), AuditMessageDTO.class);
                    } catch (IOException e) {
                        return null;
                    }
                }).filter(Objects::nonNull)
                .map(MetricUtil::convertAuditMessageDtoToMetric)
                .collect(Collectors.toList());
        try {
            auditMetricRepository.saveAll(auditMetrics);
        } catch (Exception e) {
            log.error("save audit metrics failed!", e);
            return;
        }
        AtomicLong oldDocNum = new AtomicLong();
        saveDocumentNum.accumulateAndGet(auditMetrics.size(), (left, right) -> {
            if (left + right > 1000) {
                oldDocNum.set(left + right);
                return left + right - 1000;
            }
            return left + right;
        });
        if (oldDocNum.get() > 1000) {
            log.info("number of {} documents have been save in es;", oldDocNum.get());
        }
    }
}
