package org.limbo.coco.file;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Slf4j
public class ReadWriteCacheFile<K> implements Closeable {

    private ReadWriteLock readWriteLock;
    private Lock readLock;
    private Lock writeLock;

    private AtomicReference<CacheFileStatus> status;
    /**
     * 文件存储路径
     */
    private Path cacheFilePath;

    private RandomAccessFile randomAccessFile;

    /**
     * 该文件中缓存的数据 K 主键 V 表示数据在文件的位置
     */
    private ConcurrentHashMap<K, ValueDescriptor> cachedData;

    // 缓存文件中，有效缓存数据已使用的字节数
    private AtomicLong usedBytes;

    /**
     * 缓冲区数据
     */
    private byte[] buffer;
    /**
     * 缓冲区最大大小
     */
    private int maxBufferSize;
    /**
     * 当前缓冲区大小
     */
    private int bufferSize = 0;
    /**
     * 单个缓存文件的大小，默认50M
     */
    private volatile long maxBytesPerCacheFile;

    public ReadWriteCacheFile(String filePath, long maxBytesPerCacheFile, int maxBufferSize) {
        this.usedBytes = new AtomicLong(0);
        this.cachedData = new ConcurrentHashMap<>();
        this.status = new AtomicReference<>(CacheFileStatus.NORMAL);

        this.readWriteLock = new ReentrantReadWriteLock();
        readLock = readWriteLock.readLock();
        writeLock = readWriteLock.writeLock();

        this.maxBytesPerCacheFile = maxBytesPerCacheFile;
        this.maxBufferSize = maxBufferSize;
        this.cacheFilePath = Paths.get(filePath);
        this.buffer = new byte[maxBufferSize];

        try {
            // 缓存文件已经存在的话，则重新创建
            File cacheFile = this.cacheFilePath.toFile();
            if (cacheFile.exists()) {
                cacheFile.delete();
            }
            File parentFile = cacheFile.getParentFile();
            if (!parentFile.exists()) {
                parentFile.mkdirs();
            }
            this.randomAccessFile = new RandomAccessFile(cacheFile, "rwd");
        } catch (FileNotFoundException e) {
            log.error("Cache randomAccessFile create failed due to exception", e);
            throw new CacheFileCreateException(e);
        }
    }

    public List<K> keyList() {
        try {
            writeLock.lock();
            return new ArrayList<>(cachedData.keySet());
        } catch (Exception e) {
            log.error("Unexpected exception when read cache randomAccessFile[" + this + "]", e);
            throw new ConcurrentModificationException("Unexpected exception when read cache randomAccessFile[" + this + "]");
        } finally {
            writeLock.unlock();
        }
    }

    public void put(K key, byte[] data) {
        try {
            internalPut(key, data, false);
        } catch (IOException e) {
            log.error("Error while write cache file[" + this + "]", e);
            throw new IllegalStateException("Error while write cache file[" + this + "]", e);
        }
    }

    private void internalPut(K key, byte[] data, boolean withFlush) throws IOException {
        try {
            this.writeLock.lock();
            if (data.length == 0) {
                // 数据长度为0，则为null
                ValueDescriptor oldValueDescriptor = cachedData.get(key);
                cachedData.put(key, ValueDescriptor.NULL_VALUE_DESCRIPTOR);
                if (oldValueDescriptor != null) {
                    usedBytes.addAndGet(oldValueDescriptor.length);
                }
                return;
            }

            ValueDescriptor oldValueDescriptor;
            ValueDescriptor newValueDescriptor;
            // 写入到缓冲区
            int offset = (int) (randomAccessFile.length() + bufferSize);
            int length = data.length;

            // 缓冲区 提升性能
            if ((bufferSize + length) > maxBufferSize) {
                flush();
            }
            System.arraycopy(data, 0, buffer, bufferSize, length);
            bufferSize += length;

            oldValueDescriptor = this.cachedData.get(key);
            newValueDescriptor = new ValueDescriptor(offset, length);

            this.cachedData.put(key, newValueDescriptor);
            if (oldValueDescriptor != null) {
                this.usedBytes.addAndGet(-oldValueDescriptor.length);
            }
            this.usedBytes.addAndGet(newValueDescriptor.length);

            if (withFlush) {
                this.flush();
            }
            this.checkSwap();
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * 将缓冲区内容写入文件末尾
     */
    public void flush() {
        try {
            this.writeLock.lock();
            if (bufferSize > 0) {
                randomAccessFile.seek(randomAccessFile.length());
                randomAccessFile.write(buffer, 0, bufferSize);
                bufferSize = 0;
            }
        } catch (IOException e) {
            log.error("Error while flush write cache file[" + this + "]", e);
            throw new IllegalStateException("Error while flush write cache file[" + this + "]", e);
        } finally {
            this.writeLock.unlock();
        }
    }

    public boolean remove(K key) {
        try {
            this.writeLock.lock();
            if (cachedData.containsKey(key)) {
                ValueDescriptor vd = cachedData.remove(key);
                if (vd != null) {
                    usedBytes.addAndGet(-vd.length);
                }
                return true;
            }
            return false;
        } finally {
            this.writeLock.unlock();
        }
    }

    public byte[] get(K key) {
        try {
            this.readLock.lock();
            assertCacheAlive();

            ValueDescriptor vd = this.cachedData.get(key);
            if (vd.length == 0) {
                // 数据长度为0，则为null，直接返回一个长度为0的数组
                return new byte[0];
            }

            byte[] data = new byte[vd.length];

            if (vd.offset >= randomAccessFile.length()) {
                // offset大于文件长度，则应该从buffer中读取
                int bufferOffset = (int) (vd.offset - randomAccessFile.length());
                System.arraycopy(buffer, bufferOffset, data, 0, vd.length);
            } else {
                this.randomAccessFile.seek(vd.offset);
                this.randomAccessFile.read(data, 0, vd.length);
            }

            return data;
        } catch (IOException e) {
            log.error("Error while read cache file[" + this + "]", e);
            throw new IllegalStateException("Error while read cache file[" + this + "]", e);
        } finally {
            this.readLock.unlock();
        }
    }

    public boolean contains(K key) {
        try {
            readLock.lock();
            return cachedData.containsKey(key);
        } finally {
            readLock.unlock();
        }
    }

    private void checkSwap() {
        // 在此处，文件大小 超出 maxBytesPerCacheFile 80%的时候执行swap；
        try {
            if ((this.randomAccessFile.length() + bufferSize) >= this.maxBytesPerCacheFile * 1.8) {
                try {
                    do {
                        assertCacheAlive();
                    } while (!status.compareAndSet(CacheFileStatus.NORMAL, CacheFileStatus.SWAPPING));
                    flush();// 先flush，保证所有数据都在文件中之后，再进行swap
                    doSwap();
                } finally {
                    status.compareAndSet(CacheFileStatus.SWAPPING, CacheFileStatus.NORMAL);
                }
            }
        } catch (IOException e) {
            log.error("Unexpected IOException while checkSwap", e);
            throw new IllegalStateException("Unexpected IOException while checkSwap", e);
        }
    }

    private void doSwap() {
        String cacheFilePathName = this.cacheFilePath.getName(this.cacheFilePath.getNameCount() - 1).toString();
        Path swapFilePath = this.cacheFilePath.resolveSibling(cacheFilePathName + ".swap");
        // **.data.swap
        File swapFile = swapFilePath.toFile();
        // **.data
        File cacheFile = this.cacheFilePath.toFile();
        // **.data.swap.1
        File tempSwapFile = this.cacheFilePath.resolveSibling(cacheFilePathName + ".swap.1").toFile();
        OutputStream swapOut = null;
        try {
            this.writeLock.lock();

            // 将可用数据保存到swap文件
            swapOut = new FileOutputStream(swapFile);//Files.newOutputStream(swapFilePath, StandardOpenOption.WRITE);
            byte[] data = null;
            int hasWritten = 0;
            for (Map.Entry<K, ValueDescriptor> entry : this.cachedData.entrySet()) {
                ValueDescriptor vd = entry.getValue();
                if (data == null || data.length < vd.length) {
                    data = new byte[vd.length];
                }
                randomAccessFile.seek(vd.offset);
                randomAccessFile.read(data, 0, vd.length);
                vd.offset = hasWritten;
                swapOut.write(data, 0, vd.length);
                hasWritten += vd.length;
            }

            // 关闭文件流
            IOUtils.closeQuietly(this.randomAccessFile);
            this.randomAccessFile = null;
            IOUtils.closeQuietly(swapOut);

            // 将原缓存文件命名为 swap.1
            cacheFile.renameTo(tempSwapFile);

            // 将swap文件命名为缓存文件
            swapFile.renameTo(cacheFile);

            // 删除原缓存文件
            FileUtils.deleteQuietly(tempSwapFile);
        } catch (IOException e) {
            log.error("Unexpected exception while swap cache file" + this, e);
            throw new IllegalStateException("Unexpected exception while swap cache file" + this, e);
        } finally {
            if (!cacheFile.exists()) {
                if (tempSwapFile.exists()) {
                    tempSwapFile.renameTo(cacheFile);
                } else if (swapFile.exists()) {
                    swapFile.renameTo(cacheFile);
                } else {
                    throw new IllegalStateException("Bad cache file! Cannot reset cache file!");
                }
            }
            FileUtils.deleteQuietly(swapFile);
            FileUtils.deleteQuietly(tempSwapFile);

            try {
                IOUtils.closeQuietly(this.randomAccessFile);
                this.randomAccessFile = new RandomAccessFile(cacheFile, "rwd");
            } catch (FileNotFoundException e) {
                // ?? impossible
                log.warn("未知的错误，导致swap后无法创建文件流！", e);
            }
            IOUtils.closeQuietly(swapOut);
            this.writeLock.unlock();
        }

    }

    @Override
    public void close() throws IOException {
        try {
            writeLock.lock();
            while (!status.compareAndSet(CacheFileStatus.NORMAL, CacheFileStatus.CLOSED)) {
                if (status.get() == CacheFileStatus.CLOSED) {
                    break;
                }
                log.debug("Cache randomAccessFile closed");
            }
            if (randomAccessFile != null) {
                randomAccessFile.close();
                randomAccessFile = null;
            }
            if (cacheFilePath != null) {
                cacheFilePath.toFile().delete();
            }
        } catch (IOException e) {
            log.error("Unexpected exception while close cache randomAccessFile", e);
            throw e;
        } finally {
            writeLock.unlock();
        }
    }

    public long getUsedBytes() {
        return usedBytes.get();
    }

    private void assertCacheAlive() {
        if (status.get() == CacheFileStatus.CLOSED) {
            throw new IllegalStateException("Cache file[" + this + "] is closed!");
        }
    }
}
