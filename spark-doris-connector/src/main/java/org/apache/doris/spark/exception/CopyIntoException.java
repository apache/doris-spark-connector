package org.apache.doris.spark.exception;

public class CopyIntoException extends Exception {
    public CopyIntoException() {
        super();
    }
    public CopyIntoException(String message) {
        super(message);
    }
    public CopyIntoException(String message, Throwable cause) {
        super(message, cause);
    }
    public CopyIntoException(Throwable cause) {
        super(cause);
    }
    protected CopyIntoException(String message, Throwable cause,
                                boolean enableSuppression,
                                boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
