package org.limbo.coco.file;

import lombok.extern.slf4j.Slf4j;
import org.limbo.coco.AbstractCache;
import org.limbo.coco.IndexBuilder;
import org.limbo.coco.MemoryUnit;
import org.limbo.coco.file.serializer.ValueSerializer;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 一个缓存中存在多个缓存文件；
 * 每个缓存文件维护自己的一个Map，Map中存储缓存的key和缓存value的描述对象
 * 每个缓存文件记录 有效缓存字节数，总缓存文件字节数；
 * put新增缓存时，找到可用的缓存文件，将 key 和 value描述对象存入Map，value写入缓存文件；
 * put更新缓存时，先尝试写入原缓存文件，若写入后 有效缓存字节数 小于 单个缓存文件大小限制，则在末尾写入，并更新value描述对象；
 * 若写入后 有效缓存字节数 大于 单个缓存文件大小限制，则找到新的可用缓存文件写入，并移除原来的缓存；
 * put新增或更新后，将检测 总缓存文件字节数 是否超过 单个缓存文件大小限制，若超过则发生swap，将缓存文件内的缓存对象交换到新的文件中，并抛弃原有的缓存文件；
 * <p>
 * remove缓存时，找到key所在的缓存文件，从Map中移除key和value描述对象；
 * <p>
 * get缓存时，找到key所在的缓存文件，从Map中读取key对应的value描述对象，然后从缓存文件中读取并反序列化value；
 * <p>
 * 每个缓存文件记录 有效缓存字节数，总缓存文件字节数，当总缓存文件字节数超出单个缓存文件大小限制时，发生swap；
 * 缓存文件中的缓存数据发生变动时，会在同一个缓存文件的末尾写入新的数据，并更新value描述对象；
 * 当缓存文件的 有效缓存字节数 即将超过 单个缓存文件大小限制 时，改缓存文件将不再接收新的缓存数据；
 */
@Slf4j
public class DiskCache<K, V> extends AbstractCache<K, V> implements Closeable {

    /**
     * 缓存文件目录
     */
    private String CACHE_FILE_DIRECTORY;

    /**
     * 缓存名称
     */
    private String name;

    /**
     * 缓存是否可用
     */
    private final AtomicBoolean cacheAlive = new AtomicBoolean(true);

    /**
     * 单个缓存文件大小，默认50M
     */
    private long maxBytesPerCacheFile = MemoryUnit.MEGABYTES.toBytes(50);

    /**
     * 单个缓存文件缓冲区大小，默认64K
     */
    private long maxBytesInMemoryBufferPerCacheFile = MemoryUnit.KILOBYTES.toBytes(64);

    /**
     * 缓存文件
     */
    private final Map<K, AbstractCacheFile<K>> cacheFiles;
    /**
     * 缓存文件个数
     */
    private final AtomicInteger cacheFileSize;

    private final ReentrantLock cacheFileGrowLock;

    private ValueSerializer<V> valueSerializer;

    /**
     * 缓存条目数量
     */
    private final AtomicInteger size;

    public DiskCache(String dir, String name, ValueSerializer<V> serializer) {
        this(dir, name, serializer, MemoryUnit.MEGABYTES.toBytes(50));
    }

    public DiskCache(String dir, String name, ValueSerializer<V> serializer, long maxBytesPerCacheFile) {
        this();

        this.CACHE_FILE_DIRECTORY = dir;
        this.name = name;
        this.valueSerializer = serializer;
        this.maxBytesPerCacheFile = maxBytesPerCacheFile;
    }

    public DiskCache(DiskCacheConfiguration<K, V> config) {
        this();
        this.name = config.getName();
        this.valueSerializer = config.getValueSerializer();
        this.maxBytesPerCacheFile = config.getMaxBytesPerCacheFile();
        this.maxBytesInMemoryBufferPerCacheFile = config.getMaxBytesInMemoryBufferPerCacheFile();
        for (IndexBuilder<V> ib : config.getIndexBuilders()) {
            this.addIndex(ib);
        }
    }

    public DiskCache() {
        this.name = getClass().getName() + "_" + System.currentTimeMillis();
        this.cacheFiles = new HashMap<>();
        this.cacheFileGrowLock = new ReentrantLock();
        this.size = new AtomicInteger(0);
        this.cacheFileSize = new AtomicInteger(0);
    }

    @Override
    public List<K> getKeys() {
        assertCacheAlive();
        return new ArrayList<>(cacheFiles.keySet());
    }

    @Override
    public boolean put(K key, V value) {
        AbstractCacheFile<K> cacheFileToPut = null;
        boolean exist = false;
        byte[] data = value == null ? new byte[0] : this.valueSerializer.serialize(value);
        try {
            cacheFileGrowLock.lock();
            assertCacheAlive();

            if (cacheFiles.containsKey(key)) {
                // 先查找是否已经存在key，存在则覆盖
                cacheFileToPut = cacheFiles.get(key);
                exist = true;
            } else {
                for (AbstractCacheFile<K> cacheFile : cacheFiles.values()) {
                    // 不存在则从容量足够的缓存文件中选择一个
                    // usedBytes + data.length < maxBytesPerCacheFile 即认为是缓存文件容量不足
                    long minBytesInNeed = maxBytesPerCacheFile - data.length;
                    if (cacheFile.getUsedBytes() < minBytesInNeed) {
                        cacheFileToPut = cacheFile;
                    }
                }

                // 若所有的缓存文件容量都不够，则新增一个
                if (cacheFileToPut == null) {
                    String filePath = CACHE_FILE_DIRECTORY + "/" + this.name + "_cache_" + cacheFileSize.get() + ".data";
                    cacheFileToPut = new ReentrantCacheFile<>(filePath, maxBytesPerCacheFile, (int) maxBytesInMemoryBufferPerCacheFile);
//                    cacheFileToPut = new MappedByteBufferCacheFile<>(filePath, maxBytesPerCacheFile, (int) maxBytesInMemoryBufferPerCacheFile);
                    cacheFileSize.incrementAndGet();
                }

                // key与文件的映射关系
                cacheFiles.put(key, cacheFileToPut);
            }

            putIndex(key, value);
        } finally {
            cacheFileGrowLock.unlock();
        }

        cacheFileToPut.put(key, data);
        if (!exist) {
            size.incrementAndGet();
        }
        return exist;
    }

    @Override
    public V get(K key) {
        assertCacheAlive();
        if (cacheFiles.containsKey(key)) {
            byte[] data = cacheFiles.get(key).get(key);
            return data == null ? null : this.valueSerializer.deserialize(data);
        } else {
            return null;
        }
    }

    @Override
    public boolean contains(K key) {
        assertCacheAlive();
        return cacheFiles.containsKey(key);
    }

    @Override
    public boolean remove(K key) {
        assertCacheAlive();

        V existValue = get(key);
        if (existValue == null) {
            return false;
        }

        AbstractCacheFile<K> cacheFile = cacheFiles.get(key);
        cacheFiles.remove(key);
        if (cacheFile.remove(key)) {
            size.decrementAndGet();
            removeIndex(key, existValue);
            return true;
        }

        return false;
    }

    @Override
    public int getSize() {
        return size.get();
    }

    @Override
    public void close() {
        try {
            do {
                if (!cacheAlive.get()) {
                    break;
                }
            } while (!cacheAlive.compareAndSet(true, false));
            for (AbstractCacheFile<K> cacheFile : cacheFiles.values()) {
                cacheFile.close();
            }
        } catch (IOException e) {
            log.error("IOException while close cache[" + this, e);
            throw new IllegalStateException("IOException while close cache[" + this, e);
        }
    }

    private void assertCacheAlive() {
        if (!cacheAlive.get()) {
            throw new IllegalStateException("Cache[" + this.name + "] already closed!");
        }
    }

    // ====================================================== Configuration 配置项 ==================================================
    public DiskCache<K, V> setMaxBytesPerCacheFile(int size, MemoryUnit unit) {
        this.maxBytesPerCacheFile = unit.toBytes(size);
        return this;
    }

    public DiskCache<K, V> setMaxBytesInMemoryBufferPerCacheFile(int size, MemoryUnit unit) {
        this.maxBytesInMemoryBufferPerCacheFile = unit.toBytes(size);
        return this;
    }
}
