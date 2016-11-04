package com.twitter.scalding;

/**
 * thrown when validateTaps fails
 *
 * Defined in Java as SLS 5.3.1 prevents us from selecting which inherited ctor we defer to.
 */
public class InvalidSourceException extends RuntimeException {
    public InvalidSourceException(String message) {
        super(message);
    }
    public InvalidSourceException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
