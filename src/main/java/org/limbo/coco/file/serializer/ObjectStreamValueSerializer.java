package org.limbo.coco.file.serializer;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;

import java.io.*;

/**
 * 基于Serializer的对象序列化
 * @param <V>
 */
@Slf4j
public class ObjectStreamValueSerializer<V> implements ValueSerializer<V> {

    @Override
    public byte[] serialize(V value) {
        ByteArrayOutputStream bout = null;
        ObjectOutputStream oout = null;
        try {
            oout = new ObjectOutputStream(new ByteArrayOutputStream());
            oout.writeObject(value);
            return bout.toByteArray();
        } catch (IOException e) {
            log.error("序列化JSON报错！", e);
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(oout);
            IOUtils.closeQuietly(bout);
        }
    }

    @Override
    public V deserialize(byte[] data) {
        ByteArrayInputStream bin = null;
        ObjectInputStream oin = null;
        try {
            bin = new ByteArrayInputStream(data);
            oin = new ObjectInputStream(bin);
            Object objValue = oin.readObject();
            return (V) objValue;
        } catch (Exception e) {
            log.error("反序列化JSON报错！", e);
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(oin);
            IOUtils.closeQuietly(bin);
        }
    }
}
