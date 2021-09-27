package org.limbo.coco.file.serializer;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.limbo.utils.JacksonUtils;

import java.nio.charset.StandardCharsets;

/**
 * 将对象转为json字符串的形式来序列化
 *
 * @param <V>
 */
@Slf4j
public class ClassJsonValueSerializer<V> implements ValueSerializer<V> {

    private Class<V> clazz;

    public ClassJsonValueSerializer(Class<V> clazz) {
        this.clazz = clazz;
    }

    @Override
    public byte[] serialize(V value) {
        if (value == null) {
            return new byte[0];
        }

        String jsonStr = JacksonUtils.toJSONString(value);
        try {
            return jsonStr.getBytes(StandardCharsets.UTF_8);
        } catch (RuntimeException e) {
            // ignore
            log.error("序列化JSON报错！", e);
            throw e;
        }
    }

    @Override
    public V deserialize(byte[] data) {
        if (data == null || data.length == 0) {
            return null;
        }

        try {
            String jsonStr = new String(data, StandardCharsets.UTF_8);
            return StringUtils.isBlank(jsonStr) ? null : JacksonUtils.parseObject(jsonStr, clazz);
        } catch (RuntimeException e) {
            // ignore
            log.error("反序列化JSON报错！", e);
            throw e;
        }
    }
}
