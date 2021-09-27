package org.limbo.coco.file.serializer;

import com.fasterxml.jackson.core.type.TypeReference;

/**
 * @author Devil
 * @since 2021/9/27
 */
public class SerializerFactory {

    public static <V> ValueSerializer<V> objectStream() {
        return new ObjectStreamValueSerializer<>();
    }

    public static <V> ValueSerializer<V> classJson(Class<V> clazz) {
        return new ClassJsonValueSerializer<>(clazz);
    }

    public static <V> ValueSerializer<V> typeJson(TypeReference<V> type) {
        return new TypeJsonValueSerializer<>(type);
    }

}
