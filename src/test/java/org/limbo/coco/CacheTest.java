package org.limbo.coco;

import org.junit.Test;
import org.limbo.coco.file.DiskCache;
import org.limbo.coco.file.serializer.ClassJsonValueSerializer;
import org.limbo.coco.file.serializer.SerializerFactory;

import java.util.concurrent.CountDownLatch;

public class CacheTest {

    /**
     * 固态 写 11913 读 9372
     * 机械 写       读
     */
    @Test
    public void testNoIndex() {
        DiskCache<Long, UserModel> cache = new DiskCache<>("/data/cache", "test",
                SerializerFactory.classJson(UserModel.class),
                MemoryUnit.MEGABYTES.toBytes(10));
        long t1 = System.currentTimeMillis();

        for (int i = 0; i < 100000; i++) {
            UserModel tb = new UserModel();
            tb.setUserId((long) i);
            tb.setName("" + i);
            cache.put(tb.getUserId(), tb);
        }

        System.out.println(cache.get(5L));
        System.out.println(System.currentTimeMillis() - t1); // 70555
    }

    /**
     * 原先 ReentrantCacheFile 使用 ReentrantLock write:14491 read:15656 由于多线程 锁争夺 导致耗时比单线程还要高
     * 使用 ReadWriteLock 改写 ReentrantCacheFile write:14715 read:59
     * bug 使用读写锁的时候，由于游标问题，会导致数据读取有问题
     */
    @Test
    public void testNoIndexThreads() throws InterruptedException {
        DiskCache<Long, UserModel> cache = new DiskCache<>("/data/cache", "test", new ClassJsonValueSerializer<>(UserModel.class));
        long s1 = System.currentTimeMillis();

        CountDownLatch wLatch = new CountDownLatch(3);

        new Thread(() -> {
            for (int i = 0; i < 1000000; i++) {
                UserModel tb = new UserModel();
                tb.setUserId((long) i);
                tb.setName("" + i);
                cache.put(tb.getUserId(), tb);
            }
            wLatch.countDown();
        }).start();

        new Thread(() -> {
            for (int i = 1000000; i < 2000000; i++) {
                UserModel tb = new UserModel();
                tb.setUserId((long) i);
                tb.setName("" + i);
                cache.put(tb.getUserId(), tb);
            }
            wLatch.countDown();
        }).start();

        new Thread(() -> {
            for (int i = 2000000; i < 3000000; i++) {
                UserModel tb = new UserModel();
                tb.setUserId((long) i);
                tb.setName("" + i);
                cache.put(tb.getUserId(), tb);
            }
            wLatch.countDown();
        }).start();

        wLatch.await();

        long s2 = System.currentTimeMillis();
        System.out.println("write:" + (s2 - s1));

        CountDownLatch rLatch = new CountDownLatch(3);

        new Thread(() -> {
            for (int i = 0; i < 1000000; i++) {
                cache.get((long) i);
            }
            rLatch.countDown();
        }).start();

        new Thread(() -> {
            for (int i = 1000000; i < 2000000; i++) {
                cache.get((long) i);
            }
            rLatch.countDown();
        }).start();

        new Thread(() -> {
            for (int i = 2000000; i < 3000000; i++) {
                cache.get((long) i);
            }
            rLatch.countDown();
        }).start();

        rLatch.await();

        long s3 = System.currentTimeMillis();
        System.out.println("read:" + (s3 - s2));

        cache.close();
    }

}
