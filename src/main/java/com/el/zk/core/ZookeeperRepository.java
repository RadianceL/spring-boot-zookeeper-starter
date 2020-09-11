package com.el.zk.core;

import com.el.zk.data.EventData;
import com.el.zk.lock.ShareReentryLockDealCallback;
import com.el.zk.serialize.SerializingUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
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

    /**
     * 该方法有问题
     * @param listener      路径监听器
     */
    @Deprecated
    public void startRootCache(PathChildrenCacheListener listener) {
        curatorCache = initLocalCache(rootPath, listener);
    }

    /**
     * 该方法有问题
     * @param path          路径
     * @param listener      路径监听起
     */
    @Deprecated
    public void addCacheListener(String path, PathChildrenCacheListener listener) {
        curatorCache = initLocalCache(path, listener);
    }

    /**
     * 初始化本地缓存
     * @param watchRootPath
     */
    private CuratorCache initLocalCache(String watchRootPath, PathChildrenCacheListener listener) {
        CuratorCache curatorCache = CuratorCache.builder(curatorFramework, watchRootPath).build();

        curatorCache.listenable().addListener(CuratorCacheListener.builder()
                .forAll((type, oldData, data) -> System.out.println(type))
                .forPathChildrenCache(watchRootPath, curatorFramework, listener)
                .forInitialized(() -> System.out.println("Cache initialized"))
                .build());
        curatorCache.start();
        return curatorCache;
    }

    public void startRootWatcherForUpdate(NotifyEventHandler notifyEventHandler) throws Exception {
        getClient().getZookeeperClient().getZooKeeper().addWatch(rootPath, watchedEvent -> {
            if (Watcher.Event.EventType.NodeDataChanged.equals(watchedEvent.getType())) {
                try {
                    byte[] nodeData = this.getNodeData(watchedEvent.getPath().replaceFirst(rootPath, ""));
                    notifyEventHandler.handleData(new EventData(watchedEvent.getPath(), nodeData));
                }catch (Throwable throwable) {
                    log.error("switch - update event error, data path: [{}]", watchedEvent.getPath());
                }
            }
        }, AddWatchMode.PERSISTENT_RECURSIVE);
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
            log.error("zookeeper create node exception", e);
        }
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
    public <T> void createNode(CreateMode mode, String path, T nodeData) {
        try {
            //使用creatingParentContainersIfNeeded()之后Curator能够自动递归创建所有所需的父节点
            curatorFramework.create().creatingParentsIfNeeded().withMode(mode).forPath(path, SerializingUtil.serialize(nodeData));
        } catch (Exception e) {
            log.error("zookeeper create node exception", e);
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
    public void createNode(CreateMode mode,String path) {
        try {
            //使用creatingParentContainersIfNeeded()之后Curator能够自动递归创建所有所需的父节点
            curatorFramework.create().creatingParentsIfNeeded().withMode(mode).forPath(path);
        } catch (Exception e) {
            log.error("zookeeper create node exception", e);
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
        } catch (Exception e) {
            log.error("zookeeper delete node exception", e);
        }
    }

    /**
     * 删除节点数据
     * @param path          路径
     * @param deleteChild   是否删除子节点
     */
    public void deleteNode(final String path,Boolean deleteChild){
        try {
            if(deleteChild){
                //guaranteed()删除一个节点，强制保证删除,
                // 只要客户端会话有效，那么Curator会在后台持续进行删除操作，直到删除节点成功
                curatorFramework.delete().guaranteed().deletingChildrenIfNeeded().forPath(path);
            }else{
                curatorFramework.delete().guaranteed().forPath(path);
            }
        } catch (Exception e) {
            log.error("zookeeper delete node exception", e);
        }
    }


    /**
     * 设置指定节点的数据
     * @param path
     * @param data
     */
    public void setNodeData(String path, byte[] data){
        try {
            curatorFramework.setData().forPath(path, data);
        }catch (Exception e) {
            log.error("zookeeper set node exception", e);
        }
    }


    /**
     * 设置指定节点的数据
     * @param path
     * @param data
     */
    public void setNodeData(String path, Object data){
        try {
            curatorFramework.setData().forPath(path, SerializingUtil.serialize(data));
        }catch (Exception e) {
            log.error("zookeeper set node exception", e);
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
        }catch (Exception e) {
            log.error("zookeeper get node exception", e);
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
        } catch (Exception e) {
            log.error("zookeeper judge node exist exception", e);
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
            log.error("zookeeper get child node exception", e);
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
    public <T> T getSharedReentrantLock(String lockPath,long time, ShareReentryLockDealCallback<T> dealWork){
        InterProcessMutex lock = new InterProcessMutex(curatorFramework, lockPath);
        try {
            if (!lock.acquire(time, TimeUnit.SECONDS)) {
                log.error("zookeeper get shared reentrant lock exception: could not acquire the lock");
                return null;
            }
            log.debug("{} get the lock",lockPath);
            return dealWork.deal();
        }catch(Exception e){
            log.error("zookeeper get shared reentrant lock exception", e);
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
