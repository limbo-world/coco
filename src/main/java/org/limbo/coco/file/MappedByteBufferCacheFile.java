package org.limbo.coco.file;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public class MappedByteBufferCacheFile<K> extends AbstractCacheFile<K> {

    private ReentrantLock lock;

    private FileChannel fc;

    private MappedByteBuffer mappedByteBuffer;

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

    public MappedByteBufferCacheFile(String filePath, long maxBytesPerCacheFile, int maxBufferSize) {
        super(filePath);

        this.lock = new ReentrantLock();

        this.maxBufferSize = maxBufferSize;
        this.buffer = new byte[maxBufferSize];

        try {
            this.fc = new RandomAccessFile(filePath, "rw").getChannel();
            this.mappedByteBuffer = this.fc.map(FileChannel.MapMode.READ_WRITE, 0, maxBytesPerCacheFile);
        } catch (IOException e) {
            log.error("Cache randomAccessFile create failed due to exception", e);
            throw new CacheFileCreateException(e);
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
                mappedByteBuffer.put(data);
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

            cachedData.put(key, newValueDescriptor);
            if (oldValueDescriptor == null) {
                usedBytes.addAndGet(newValueDescriptor.length);
            }

        } finally {
            unlock();
        }
    }

    /**
     * 将缓冲区数据刷入mbb
     */
    private void flush() {
        if (bufferSize > 0) {
            mappedByteBuffer.put(buffer);
            bufferSize = 0;
            buffer = new byte[maxBufferSize];
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

    @Override
    public byte[] get(K key) {
        try {
            this.lock.lock();
            assertCacheFilAlive();

            ValueDescriptor vd = this.cachedData.get(key);
            if (vd.length == 0) {
                // 数据长度为0，则为null，直接返回一个长度为0的数组
                return new byte[0];
            }

            byte[] data = new byte[vd.length];
            mappedByteBuffer.position(vd.offset);
            mappedByteBuffer.get(data, 0, vd.length);
            return data;
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        try {
            lock.lock();
            while (!status.compareAndSet(CacheFileStatus.NORMAL, CacheFileStatus.CLOSED)) {
                if (status.get() == CacheFileStatus.CLOSED) {
                    break;
                }
                log.debug("Cache randomAccessFile closed");
            }
            if (fc != null) {
                fc.close();
                fc = null;
            }
            if (cacheFilePath != null) {
                cacheFilePath.toFile().delete();
            }
        } catch (IOException e) {
            log.error("Unexpected exception while close cache randomAccessFile", e);
            throw e;
        } finally {
            lock.unlock();
        }
    }

}
