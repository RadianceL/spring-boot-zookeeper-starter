package com.el.zk.data;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * 事件对象 <br/>
 * since 2020/9/11
 *
 * @author eddie.lys
 */
@Data
@AllArgsConstructor
public class EventData {

    private String path;

    private byte[] data;
}
