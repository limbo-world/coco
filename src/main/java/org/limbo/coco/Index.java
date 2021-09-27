package org.limbo.coco;

import lombok.Getter;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class Index<K, V> {

    @Getter
    private String name;

    @Getter
    private IndexBuilder<V> indexBuilder;

    private ConcurrentHashMap<Object, Set<K>> indexedKeys;


    public Index(IndexBuilder<V> indexBuilder) {
        this.name = indexBuilder.name();
        this.indexBuilder = indexBuilder;
        this.indexedKeys = new ConcurrentHashMap<>();
    }

    public Set<K> getKeys(Object indexAttr) {
        return indexedKeys.get(indexAttr);
    }

    public void addKey(Object indexAttr, K key) {
        Set<K> keySet = indexedKeys.get(indexAttr);
        if (keySet == null) {
            HashSet<K> newOne = new HashSet<>();
            keySet = indexedKeys.putIfAbsent(indexAttr, newOne);
            keySet = keySet == null ? newOne : keySet;
        }
        keySet.add(key);
    }

    public void removeKey(Object indexAttr, K key) {
        Set<K> keySet = indexedKeys.get(indexAttr);
        if (keySet != null) {
            keySet.remove(key);
        }
    }
}
