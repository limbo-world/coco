package org.limbo.coco;

import org.apache.commons.collections.CollectionUtils;

import java.util.*;

public abstract class AbstractCache<K, V> implements Cache<K, V> {

    /** 索引 */
    private Map<String, Index<K, V>> indices;

    protected AbstractCache() {
        this.indices = new HashMap<>();
    }

    protected void putIndex(K key, V value) {
        for (Index<K, V> index : this.indices.values()) {
            Object indexAttr = index.getIndexBuilder().buildIndex(value);
            index.addKey(indexAttr, key);
        }
    }

    protected void removeIndex(K key, V value) {
        for (Index<K, V> index : this.indices.values()) {
            Object indexAttr = index.getIndexBuilder().buildIndex(value);
            index.removeKey(indexAttr, key);
        }
    }

    @Override
    public List<V> getByIndex(String indexName, Object indexAttr) {
        Index<K, V> index = indices.get(indexName);
        if (index == null) {
            throw new IllegalArgumentException(String.format("Index[%s] is not exist!", indexName));
        }

        Set<K> keys = index.getKeys(indexAttr);
        if (CollectionUtils.isEmpty(keys)) {
            return new ArrayList<>();
        }
        List<V> values = new ArrayList<>(keys.size());
        for (K k : keys) {
            values.add(get(k));
        }
        return values;
    }

    public Cache<K, V> addIndex(IndexBuilder<V> indexBuilder) {
        Index<K, V> index = new Index<>(indexBuilder);
        this.indices.put(indexBuilder.name(), index);
        return this;
    }

}
