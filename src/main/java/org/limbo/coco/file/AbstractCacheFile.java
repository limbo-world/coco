package org.limbo.coco.file;

import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public abstract class AbstractCacheFile<K> implements Closeable {

    protected AtomicReference<CacheFileStatus> status;

    /**
     * 文件存储路径
     */
    protected Path cacheFilePath;

    /**
     * 该文件中缓存的数据 K 主键 V 表示数据在文件的位置
     */
    protected Map<K, ValueDescriptor> cachedData;

    /**
     * 缓存文件中，有效缓存数据已使用的字节数
     */
    protected AtomicLong usedBytes;

    public AbstractCacheFile(String filePath) {
        this.cacheFilePath = Paths.get(filePath);
        this.cachedData = new ConcurrentHashMap<>();
        this.usedBytes = new AtomicLong(0);
        this.status = new AtomicReference<>(CacheFileStatus.NORMAL);

        // 缓存文件已经存在的话，删除旧的
        File cacheFile = cacheFilePath.toFile();
        if (cacheFile.exists()) {
            cacheFile.delete();
        }
        File parentFile = cacheFile.getParentFile();
        if (!parentFile.exists()) {
            parentFile.mkdirs();
        }
    }

    /**
     * 确保缓存文件可用
     */
    protected void assertCacheFilAlive() {
        if (status.get() == CacheFileStatus.CLOSED) {
            throw new IllegalStateException("Cache file[" + this + "] is closed!");
        }
    }

    /**
     * 返回所有索引值
     */
    public List<K> keyList() {
        try {
            lock();
            return new ArrayList<>(cachedData.keySet());
        } catch (Exception e) {
            log.error("Unexpected exception when read cache randomAccessFile[" + this + "]", e);
            throw new ConcurrentModificationException("Unexpected exception when read cache randomAccessFile[" + this + "]");
        } finally {
            unlock();
        }
    }

    /**
     * 判断是否包含某个索引
     */
    public boolean contains(K key) {
        try {
            lock();
            return cachedData.containsKey(key);
        } finally {
            unlock();
        }
    }

    public long getUsedBytes() {
        return usedBytes.get();
    }

    /**
     * 移除索引信息就行，不删数据
     */
    public boolean remove(K key) {
        try {
            lock();
            if (cachedData.containsKey(key)) {
                ValueDescriptor vd = cachedData.remove(key);
                if (vd != null) {
                    usedBytes.addAndGet(-vd.length);
                }
                return true;
            }
            return false;
        } finally {
            unlock();
        }
    }

    public abstract void put(K key, byte[] data);

    public abstract byte[] get(K key);

    public abstract void lock();

    public abstract void unlock();

}
