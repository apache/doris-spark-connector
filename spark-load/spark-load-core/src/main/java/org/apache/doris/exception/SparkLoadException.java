package org.apache.doris.exception;

public class SparkLoadException extends Exception {

    public SparkLoadException(String message) {
        super(message);
    }

    public SparkLoadException(String message, Throwable cause) {
        super(message, cause);
    }
}
