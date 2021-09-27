package org.limbo.coco.file.serializer;

public interface ValueSerializer<V> {

    byte [] serialize(V value);

    V deserialize(byte[] data);

}
