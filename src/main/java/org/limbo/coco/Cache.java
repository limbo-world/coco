package org.limbo.coco;

import java.io.Closeable;
import java.util.List;

public interface Cache<K, V> extends Closeable {

    /**
     * 获取所有缓存的key
     */
    List<K> getKeys();

    /**
     * 将一个k-v放入缓存，或更新缓存；
     *
     * @return 若put之前存在key，则返回true；否则返回false；
     */
    boolean put(K key, V value);

    /**
     * 根据key获取value，缓存中不存在key则返回null
     */
    V get(K key);

    /**
     * 移除key，如果key存在则返回true，否则返回false
     */
    boolean remove(K key);

    /**
     * 缓存中是否存在指定key
     */
    boolean contains(K key);

    /**
     * 获取缓存数据条数
     */
    int getSize();

    /**
     * 关闭缓存，当不使用时应该关闭缓存以释放资源
     */
    @Override
    void close();

    /**
     * 根据索引获取缓存数据
     */
    List<V> getByIndex(String indexName, Object indexAttr);
}
