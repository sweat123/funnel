package com.laomei.middleware.funnelserver.dao;

import com.laomei.middleware.funnelserver.AuditMetric;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

/**
 * @author laomei on 2018/8/13 23:21
 */
public interface AuditMetricRepository extends ElasticsearchRepository<AuditMetric, Long> {

}
