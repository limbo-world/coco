package org.limbo.coco.memory;

import org.limbo.coco.AbstractCache;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class MapCache<K, V> extends AbstractCache<K, V> {

    private final ConcurrentHashMap<K, V> internalMap;

    public MapCache() {
        this.internalMap = new ConcurrentHashMap<>();
    }

    @Override
    public List<K> getKeys() {
        return new LinkedList<>(this.internalMap.keySet());
    }

    @Override
    public synchronized boolean put(K key, V value) {
        boolean existed = internalMap.putIfAbsent(key, value) != null;
        putIndex(key, value);
        return existed;
    }

    @Override
    public V get(K key) {
        return internalMap.get(key);
    }

    @Override
    public synchronized boolean remove(K key) {
        V existed = internalMap.remove(key);
        if (existed != null) {
            removeIndex(key, existed);
        }
        return existed != null;
    }

    @Override
    public boolean contains(K key) {
        return internalMap.containsKey(key);
    }

    @Override
    public int getSize() {
        return internalMap.size();
    }

    @Override
    public void close() {
        this.internalMap.clear();
    }
}
