package org.limbo.coco.file;

public enum CacheFileStatus {

    /** 正常读写中 */
    NORMAL,
    /** 缓存交换中 */
    SWAPPING,
    /** 缓存文件已关闭，缓存失效 */
    CLOSED,
    ;

}
