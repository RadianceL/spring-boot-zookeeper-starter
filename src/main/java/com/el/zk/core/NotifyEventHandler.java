package com.el.zk.core;

import com.el.zk.data.EventData;

/**
 * 通知监听 <br/>
 * since 2020/9/11
 *
 * @author eddie.lys
 */
@FunctionalInterface
public interface NotifyEventHandler {

    /**
     * 事件处理器
     * @param childData
     */
    void handleData(EventData childData);

}
