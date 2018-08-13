package com.laomei.middleware.funnelserver.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * @author laomei on 2018/8/13 21:38
 */
@Data
@Component
@ConfigurationProperties(prefix = "funnel.server")
public class FunnelServerConfig {

    /**
     * 审计的 Topic
     */
    private String auditCenterTopic;
}
