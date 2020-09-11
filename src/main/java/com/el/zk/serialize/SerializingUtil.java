package com.el.zk.serialize;

import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.runtime.RuntimeSchema;

/**
 * 序列化工具 <br/>
 * since 2020/9/9
 *
 * @author eddie.lys
 */
public class SerializingUtil {

    /**
     * 将目标类序列化为byte数组
     */
    public static <T> byte[] serialize(T source) {
        RuntimeSchema<T> schema;
        LinkedBuffer buffer = null;
        byte[] result;
        try {
            schema = RuntimeSchema.createFrom((Class<T>) source.getClass());
            buffer = LinkedBuffer.allocate(LinkedBuffer.DEFAULT_BUFFER_SIZE);
            result = ProtostuffIOUtil.toByteArray(source, schema, buffer);
        } catch (Exception e) {
            throw new RuntimeException("serialize exception");
        } finally {
            if (buffer != null) {
                buffer.clear();
            }
        }

        return result;
    }

    /**
     * 将byte数组反序列化为目标类
     */
    public static <T> T deserialize(byte[] source, Class<T> typeClass) {
        RuntimeSchema<T> schema;
        T newInstance;
        try {
            schema = RuntimeSchema.createFrom(typeClass);
            newInstance = typeClass.newInstance();
            ProtostuffIOUtil.mergeFrom(source, newInstance, schema);
        } catch (Exception e) {
            throw new RuntimeException("deserialize exception");
        }

        return newInstance;
    }
}
