package org.limbo.coco.file;

import lombok.Getter;
import org.limbo.coco.IndexBuilder;
import org.limbo.coco.MemoryUnit;
import org.limbo.coco.file.serializer.ValueSerializer;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

@Getter
public class DiskCacheConfiguration<K, V> {

    private String name;

    /** 单个缓存文件的大小，默认50M */
    private long maxBytesPerCacheFile = MemoryUnit.MEGABYTES.toBytes(50);

    /** 每个缓存文件在内存中缓存的数据长度 */
    private long maxBytesInMemoryBufferPerCacheFile = MemoryUnit.KILOBYTES.toBytes(64);

    private ValueSerializer<V> valueSerializer;

    private List<IndexBuilder<V>> indexBuilders;

    public DiskCacheConfiguration() {
        this.indexBuilders = new LinkedList<>();
    }

    public DiskCacheConfiguration<K, V> setName(String name) {
        this.name = name;
        return this;
    }

    public DiskCacheConfiguration<K, V> setMaxBytesPerCacheFile(long maxBytesPerCacheFile) {
        this.maxBytesPerCacheFile = maxBytesPerCacheFile;
        return this;
    }

    public DiskCacheConfiguration<K, V> setMaxBytesInMemoryBufferPerCacheFile(long maxBytesInMemoryBuffer) {
        this.maxBytesInMemoryBufferPerCacheFile = maxBytesInMemoryBuffer;
        return this;
    }

    public DiskCacheConfiguration<K, V> setValueSerializer(ValueSerializer<V> valueSerializer) {
        this.valueSerializer = valueSerializer;
        return this;
    }

    public DiskCacheConfiguration<K, V> addIndices(Collection<IndexBuilder<V>> indexBuilders) {
        this.indexBuilders.addAll(indexBuilders);
        return this;
    }

    public DiskCacheConfiguration<K, V> addIndex(IndexBuilder<V> indexBuilder) {
        this.indexBuilders.add(indexBuilder);
        return this;
    }
}
