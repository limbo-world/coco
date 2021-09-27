package org.limbo.coco.file;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public class ReentrantCacheFile<K> extends AbstractCacheFile<K> {

    private ReentrantLock lock;

    private RandomAccessFile randomAccessFile;

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

    public ReentrantCacheFile(String filePath, long maxBytesPerCacheFile, int maxBufferSize) {
        super(filePath);

        this.lock = new ReentrantLock();
        this.maxBytesPerCacheFile = maxBytesPerCacheFile;
        this.maxBufferSize = maxBufferSize;
        this.buffer = new byte[maxBufferSize];

        try {
            this.randomAccessFile = new RandomAccessFile(cacheFilePath.toFile(), "rwd");
        } catch (FileNotFoundException e) {
            log.error("Cache randomAccessFile create failed due to exception", e);
            throw new CacheFileCreateException(e);
        }
    }

    /**
     * 将缓冲区内容写入文件末尾
     */
    public void flush() {
        try {
            lock();
            if (bufferSize > 0) {
                randomAccessFile.seek(randomAccessFile.length());
                randomAccessFile.write(buffer, 0, bufferSize);
                bufferSize = 0;
            }
        } catch (IOException e) {
            log.error("Error while flush write cache file[" + this + "]", e);
            throw new IllegalStateException("Error while flush write cache file[" + this + "]", e);
        } finally {
            unlock();
        }
    }

    @Override
    public byte[] get(K key) {
        try {
            lock();
            assertCacheFilAlive();
            ValueDescriptor vd = cachedData.get(key);
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
            unlock();
        }
    }

    private void checkSwap() {
        // 在此处，文件大小 超出 maxBytesPerCacheFile 80%的时候执行swap；
        try {
            lock();
            if ((this.randomAccessFile.length() + bufferSize) >= this.maxBytesPerCacheFile * 1.8) {
                try {
                    do {
                        assertCacheFilAlive();
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
        } finally {
            unlock();
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
            lock();

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
            unlock();
        }

    }

    @Override
    public void close() throws IOException {
        try {
            lock();
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
            unlock();
        }
    }

    @Override
    public void put(K key, byte[] data) {
        try {
            lock();
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
            int length = data.length;

            if (length > maxBufferSize) { // 如果单个值已经超过缓冲区大小了直接写入文件
                flush(); // 先flush 保证数据位置
                randomAccessFile.seek(randomAccessFile.length());
                randomAccessFile.write(data, 0, length);
            } else {
                // 缓冲区 提升性能
                if ((bufferSize + length) > maxBufferSize) {
                    flush();
                }
                System.arraycopy(data, 0, buffer, bufferSize, length);
                bufferSize += length;
            }

            oldValueDescriptor = this.cachedData.get(key);
            newValueDescriptor = new ValueDescriptor(usedBytes.intValue(), length);

            this.cachedData.put(key, newValueDescriptor);
            if (oldValueDescriptor == null) {
                this.usedBytes.addAndGet(newValueDescriptor.length);
            }
            this.checkSwap();
        } catch (IOException e) {
            log.error("Error while write buffer in cache file[" + this + "]", e);
            throw new IllegalStateException("Error while write buffer in cache file[" + this + "]", e);
        } finally {
            unlock();
        }
    }

    @Override
    public void lock() {
        lock.lock();
    }

    @Override
    public void unlock() {
        lock.unlock();
    }
}
