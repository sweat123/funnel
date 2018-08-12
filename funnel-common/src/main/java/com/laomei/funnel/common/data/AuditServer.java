package com.laomei.funnel.common.data;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 审核客户端相关的信息；
 * 如 service => 被审核的客户端属于什么服务
 * @author laomei on 2018/8/12 10:22
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AuditServer {

    /**
     * 客户端所在的服务
     */
    private String service;

    /**
     * 客户端所在服务器的 ip
     */
    private String ip;
}
