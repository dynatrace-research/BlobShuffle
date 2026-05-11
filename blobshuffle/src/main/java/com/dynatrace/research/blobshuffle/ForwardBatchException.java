package com.dynatrace.research.blobshuffle;

/**
 * Thrown when forwarding a read or write request to a remote cache cluster member fails.
 */
class ForwardBatchException extends RuntimeException {

    ForwardBatchException(String message, Throwable cause) {
        super(message, cause);
    }

    ForwardBatchException(String message) {
        super(message);
    }

}
