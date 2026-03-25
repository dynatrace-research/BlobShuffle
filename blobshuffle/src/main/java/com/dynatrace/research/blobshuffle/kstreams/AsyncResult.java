package com.dynatrace.research.blobshuffle.kstreams;

public class AsyncResult<T> {

    private final T result;

    private final Throwable exception;

    private AsyncResult(T result, Throwable exception) {
        this.result = result;
        this.exception = exception;
    }

    public T getResult() {
        return result;
    }

    public Throwable getException() {
        return exception;
    }

    public boolean isFailed() {
        return this.exception != null;
    }

    public static <T> AsyncResult<T> of(T result) {
        return new AsyncResult<>(result, null);
    }

    public static <T> AsyncResult<T> ofFailed(Throwable exception) {
        return new AsyncResult<>(null, exception);
    }

}
