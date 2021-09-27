package org.limbo.coco;

public interface IndexBuilder<V> {

    String name();

    Object buildIndex(V value);

}
