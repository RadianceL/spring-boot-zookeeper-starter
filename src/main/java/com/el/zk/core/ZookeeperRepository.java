package com.el.zk.core;

import com.el.zk.lock.ShareReentryLockDealCallback;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.zookeeper.CreateMode;
import org.springframework.beans.factory.annotation.Autowired;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * zk存储连接器 <br/>
 * since 2020/9/2
 *
 * @author eddie.lys
 */
@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class ZookeeperRepository {

    private final CuratorFramework curatorFramework;

    private final String rootPath;

    private CuratorCache curatorCache;

    private void start() {
        curatorCache = initLocalCache("rootPath", ((type, oldData, data) -> {
            // ignore
        }));
    }

    /**
     * 初始化本地缓存
     * @param watchRootPath
     */
    public CuratorCache initLocalCache(String watchRootPath, CuratorCacheListener listener) {
        CuratorCache curatorCache = CuratorCache.builder(curatorFramework, watchRootPath).build();
        curatorCache.listenable().addListener(listener);
        curatorCache.start();
        return curatorCache;
    }

    public CuratorFramework getClient() {
        return this.curatorFramework;
    }

    public void close() {
        this.curatorFramework.close();
    }

    /**
     * 创建节点
     * @param mode       节点类型
     * 1、PERSISTENT 持久化目录节点，存储的数据不会丢失。
     * 2、PERSISTENT_SEQUENTIAL顺序自动编号的持久化目录节点，存储的数据不会丢失
     * 3、EPHEMERAL临时目录节点，一旦创建这个节点的客户端与服务器端口也就是session 超时，这种节点会被自动删除
     *4、EPHEMERAL_SEQUENTIAL临时自动编号节点，一旦创建这个节点的客户端与服务器端口也就是session 超时，这种节点会被自动删除，并且根据当前已近存在的节点数自动加 1，然后返回给客户端已经成功创建的目录节点名。
     * @param path  节点名称
     * @param nodeData  节点数据
     */
    public void createNode(CreateMode mode, String path , String nodeData) {
        try {
            //使用creatingParentContainersIfNeeded()之后Curator能够自动递归创建所有所需的父节点
            curatorFramework.create().creatingParentsIfNeeded().withMode(mode).forPath(path,nodeData.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            log.error("注册出错", e);
        }
    }

    /**
     * 创建节点
     * @param mode       节点类型
     *                   1、PERSISTENT 持久化目录节点，存储的数据不会丢失。
     *                   2、PERSISTENT_SEQUENTIAL顺序自动编号的持久化目录节点，存储的数据不会丢失
     *                   3、EPHEMERAL临时目录节点，一旦创建这个节点的客户端与服务器端口也就是session 超时，这种节点会被自动删除
     *                   4、EPHEMERAL_SEQUENTIAL临时自动编号节点，一旦创建这个节点的客户端与服务器端口也就是session 超时，这种节点会被自动删除，并且根据当前已近存在的节点数自动加 1，然后返回给客户端已经成功创建的目录节点名。
     * @param path  节点名称
     */
    public void createNode(CreateMode mode,String path ) {
        try {
            //使用creatingParentContainersIfNeeded()之后Curator能够自动递归创建所有所需的父节点
            curatorFramework.create().creatingParentsIfNeeded().withMode(mode).forPath(path);
        } catch (Exception e) {
            log.error("注册出错", e);
        }
    }

    /**
     * 删除节点数据
     *
     * @param path
     */
    public void deleteNode(final String path) {
        try {
            deleteNode(path,true);
        } catch (Exception ex) {
            log.error("删除节点异常", ex);
        }
    }


    /**
     * 删除节点数据
     * @param path
     * @param deleteChildre   是否删除子节点
     */
    public void deleteNode(final String path,Boolean deleteChildre){
        try {
            if(deleteChildre){
                //guaranteed()删除一个节点，强制保证删除,
                // 只要客户端会话有效，那么Curator会在后台持续进行删除操作，直到删除节点成功
                curatorFramework.delete().guaranteed().deletingChildrenIfNeeded().forPath(path);
            }else{
                curatorFramework.delete().guaranteed().forPath(path);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * 设置指定节点的数据
     * @param path
     * @param datas
     */
    public void setNodeData(String path, byte[] datas){
        try {
            curatorFramework.setData().forPath(path, datas);
        }catch (Exception ex) {
            log.error("设置节点数据异常",ex);
        }
    }

    /**
     * 获取指定节点的数据
     * @param path
     * @return
     */
    public byte[] getNodeData(String path){
        try {
            if(curatorCache != null){
                Optional<ChildData> data = curatorCache.get(path);
                ChildData childData = data.orElse(null);
                if (Objects.isNull(childData)) {
                    return null;
                }
                return childData.getData();
            }
            return curatorFramework.getData().forPath(path);
        }catch (Exception ex) {
            log.error("获取指定节点数据异常",ex);
        }
        return null;
    }

    /**
     * 获取数据时先同步
     * @param path
     * @return
     */
    public byte[] synNodeData(String path){
        curatorFramework.sync();
        return getNodeData(path);
    }

    /**
     * 判断路径是否存在
     *
     * @param path
     * @return
     */
    public boolean isExistNode(final String path) {
        curatorFramework.sync();
        try {
            return null != curatorFramework.checkExists().forPath(path);
        } catch (Exception ex) {
            return false;
        }
    }


    /**
     * 获取节点的子节点
     * @param path
     * @return
     */
    public List<String> getChildren(String path) {
        List<String> childrenList = new ArrayList<>();
        try {
            childrenList = curatorFramework.getChildren().forPath(path);
        } catch (Exception e) {
            log.error("获取子节点出错", e);
        }
        return childrenList;
    }

    /**
     * 可重入共享锁  -- Shared Reentrant Lock
     * @param lockPath
     * @param time
     * @param dealWork 获取
     * @return
     */
    public Object getSRLock(String lockPath,long time, ShareReentryLockDealCallback<?> dealWork){
        InterProcessMutex lock = new InterProcessMutex(curatorFramework, lockPath);
        try {
            if (!lock.acquire(time, TimeUnit.SECONDS)) {
                log.error("get lock fail:{}", " could not acquire the lock");
                return null;
            }
            log.debug("{} get the lock",lockPath);
            return dealWork.deal();
        }catch(Exception e){
            log.error("{}", e);
        }finally{
            try {
                lock.release();
            } catch (Exception ignored) {
            }
        }
        return null;
    }

    /**
     * 获取读写锁
     * @param path
     * @return
     */
    public InterProcessReadWriteLock getReadWriteLock(String path){
        return new InterProcessReadWriteLock(curatorFramework, path);
    }


}