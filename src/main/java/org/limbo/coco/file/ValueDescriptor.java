package org.limbo.coco.file;

import lombok.ToString;

/**
 * 值的描述信息
 */
@ToString
public class ValueDescriptor {

    public static final ValueDescriptor NULL_VALUE_DESCRIPTOR = new ValueDescriptor(0, 0);

    /** 缓存偏移量 */
    protected int offset;

    /** 缓存数据长度 */
    protected int length;

    public ValueDescriptor(int offset, int length) {
        this.offset = offset;
        this.length = length;
    }
}
