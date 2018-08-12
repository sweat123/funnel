package com.laomei.funnel.client.core;

/**
 * @author laomei on 2018/7/29 16:12
 */
public class ConfigConstants {

    public static final String TIME_BUCKET_INTERVAL = "fennel.time.bucket.interval";

    /**
     * 用于传输审计内容的 topic
     */
    public static final String AUDIT_CENTER_TOPIC = "funnel.audit.center.topic";

    /**
     * 客户端 group id;
     * 生产者不需要此配置
     */
    public static final String AUDIT_CLIENT_GROUP_ID = "funnel.audit.client.group.id";

    /**
     * 客户端所属的服务
     */
    public static final String AUDIT_CLIENT_SERVICE = "funnel.audit.client.service";

    /**
     * 客户端的 ip
     */
    public static final String AUDIT_CLIENT_IP = "funnel.audit.client.ip";
}
