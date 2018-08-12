package com.laomei.funnel.common.data;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * the dto of the audit message transport in audit topic
 * @author laomei on 2018/8/12 15:56
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AuditMessageDTO {

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
