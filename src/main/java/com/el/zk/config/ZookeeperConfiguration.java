package com.el.zk.config;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 配置核心配置 <br/>
 * since 2020/9/2
 *
 * @author eddie.lys
 */
@Slf4j
@Data
@ConfigurationProperties("spring.el-util.zookeeper")
public class ZookeeperConfiguration {
    /**
     * 是否开启
     */
    private Boolean enable;
    /**
     * 服务列表
     */
    private String serverAddr;
    /**
     * 命名空间
     */
    private String namespace;
    /**
     * 项目根节点
     */
    private String rootPath;
    /**
     * acl
     */
    private String digest;
    /**
     * 会话超时时间，单位为毫秒，默认60000ms,连接断开后，其它客户端还能请到临时节点的时间
     */
    private Integer sessionTimeoutMs;
    /**
     * 连接创建超时时间，单位为毫秒
     */
    private Integer connectionTimeoutMs;
    /**
     * 最大重试次数
     */
    private Integer maxRetries;
    /**
     * 初始sleep时间 ,毫秒
     */
    private Integer baseSleepTimeMs;
}
