package org.limbo.coco;


import org.apache.commons.collections.CollectionUtils;

import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

public class IndexedSet<E> extends AbstractSet<E> implements Iterable<E> {

    private Set<E> internalSet;

    /** 索引 */
    private Map<String, Index<E, E>> indices;

    private ReentrantLock lock;

    public IndexedSet() {
        this.internalSet = new HashSet<>();
        this.indices = new HashMap<>();

        this.lock = new ReentrantLock();
    }

    @Override
    public Iterator<E> iterator() {
        return internalSet.iterator();
    }

    @Override
    public int size() {
        return internalSet.size();
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        this.lock.lock();

        try {
            boolean modified = false;
            for (E e : c)
                if (internalSet.add(e)) {
                    putIndex(e);
                    modified = true;
                } else {
                    removeIndex(e);
                    putIndex(e);
                }
            return modified;
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public boolean add(E e) {
        this.lock.lock();

        try {
            if (internalSet.add(e)) {
                putIndex(e);
                return true;
            } else {
                removeIndex(e);
                putIndex(e);
                return false;
            }
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public boolean remove(Object o) {
        this.lock.lock();

        try {
            if (internalSet.remove(o)) {
                removeIndex((E) o);
                return true;
            }

            return false;
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * 根据索引获取缓存数据
     */
    public List<E> getByIndex(String indexName, Object indexAttr) {
        Index<E, E> index = indices.get(indexName);
        if (index == null) {
            throw new IllegalArgumentException(String.format("Index[%s] is not exist!", indexName));
        }

        Set<E> keys = index.getKeys(indexAttr);
        if (CollectionUtils.isEmpty(keys)) {
            return new ArrayList<>();
        }

        return new ArrayList<>(keys);
    }

    private void putIndex(E key) {
        for (Index<E, E> index : this.indices.values()) {
            Object indexAttr = index.getIndexBuilder().buildIndex(key);
            index.addKey(indexAttr, key);
        }
    }

    private void removeIndex(E key) {
        for (Index<E, E> index : this.indices.values()) {
            Object indexAttr = index.getIndexBuilder().buildIndex(key);
            index.removeKey(indexAttr, key);
        }
    }

    public void addIndex(IndexBuilder<E> indexBuilder) {
        Index<E, E> index = new Index<>(indexBuilder);
        this.indices.put(indexBuilder.name(), index);
    }

}
