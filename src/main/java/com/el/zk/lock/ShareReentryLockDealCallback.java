package com.el.zk.lock;

/**
 * 获取可重入共享锁后的处理方法 <br/>
 * since 2020/9/2
 *
 * @author eddie.lys
 */
public interface ShareReentryLockDealCallback<T> {

    /**
     * 获取可重入共享锁后的处理方法
     * @return
     */
    T deal();

}
