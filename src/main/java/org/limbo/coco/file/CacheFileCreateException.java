package org.limbo.coco.file;

public class CacheFileCreateException extends RuntimeException {

    public CacheFileCreateException() {
    }

    public CacheFileCreateException(String message) {
        super(message);
    }

    public CacheFileCreateException(String message, Throwable cause) {
        super(message, cause);
    }

    public CacheFileCreateException(Throwable cause) {
        super(cause);
    }

    public CacheFileCreateException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
