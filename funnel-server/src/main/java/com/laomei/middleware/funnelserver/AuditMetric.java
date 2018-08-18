package com.laomei.middleware.funnelserver;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

/**
 * the dto of the audit message transport in audit topic
 * @author laomei on 2018/8/12 15:56
 */
@Data
@Document(indexName = "funnel", type = "metric")
public class AuditMetric {

    private static final long serialVersionUID = -1L;

    @Id
    @Field(type = FieldType.Auto)
    private Long id;

    private String topic;

    private byte part;

    private String groupId;

    private String ip;

    private String service;

    private Long begin;

    private Long end;

    private Long msgCount;

    private Double latencyMean;

    private Double latencyMax;

    private Double latencyC99;

    private Double latencyC95;
}
