package com.el.zk.config;

import com.el.zk.core.ZookeeperRepository;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * <br/>
 * since 2020/9/2
 *
 * @author eddie.lys
 */
public class AutoConnectZookeeperClient {

    @Bean(initMethod = "start")
    @ConditionalOnProperty(prefix = "spring.el-util.zookeeper", name = "enable")
    public ZookeeperRepository createZookeeperRepository(ZookeeperConfiguration zookeeperConfiguration) {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(zookeeperConfiguration.getBaseSleepTimeMs(),
                zookeeperConfiguration.getMaxRetries());
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder()
                .connectString(zookeeperConfiguration.getServerAddr()).retryPolicy(retryPolicy)
                .sessionTimeoutMs(zookeeperConfiguration.getSessionTimeoutMs())
                .connectionTimeoutMs(zookeeperConfiguration.getConnectionTimeoutMs())
                .namespace(zookeeperConfiguration.getNamespace());
        if(StringUtils.isNotEmpty(zookeeperConfiguration.getDigest())){
            builder.authorization("digest", zookeeperConfiguration.getDigest().getBytes(StandardCharsets.UTF_8));
            builder.aclProvider(new ACLProvider() {
                @Override
                public List<ACL> getDefaultAcl() {
                    return ZooDefs.Ids.CREATOR_ALL_ACL;
                }

                @Override
                public List<ACL> getAclForPath(final String path) {
                    return ZooDefs.Ids.CREATOR_ALL_ACL;
                }
            });
        }
        CuratorFramework curatorFramework = builder.build();

        String rootPath = "/";
        if (StringUtils.isNotBlank(zookeeperConfiguration.getRootPath())) {
            rootPath = zookeeperConfiguration.getRootPath();
        }

        return new ZookeeperRepository(curatorFramework, rootPath);
    }
}
