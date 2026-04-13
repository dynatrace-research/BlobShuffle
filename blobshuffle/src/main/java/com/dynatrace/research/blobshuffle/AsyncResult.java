package com.dynatrace.research.blobshuffle;

/**
 * Holds the outcome of an async operation: either a successful result or a failure.
 *
 * @param <T> the result type
 */
public class AsyncResult<T> {

    private final T result;

    private final Throwable exception;

    private AsyncResult(T result, Throwable exception) {
        this.result = result;
        this.exception = exception;
    }

    /**
     * Returns the result of the operation, or {@code null} if the operation failed.
     *
     * @return the result, or {@code null}
     */
    public T getResult() {
        return result;
    }

    /**
     * Returns the exception that caused the operation to fail, or {@code null} on success.
     *
     * @return the exception, or {@code null}
     */
    public Throwable getException() {
        return exception;
    }

    /**
     * Returns {@code true} if the operation failed.
     *
     * @return {@code true} if the operation failed
     */
    public boolean isFailed() {
        return this.exception != null;
    }

    /**
     * Creates a successful result.
     *
     * @param <T>    the result type
     * @param result the result value
     * @return a successful {@link AsyncResult}
     */
    public static <T> AsyncResult<T> of(T result) {
        return new AsyncResult<>(result, null);
    }

    /**
     * Creates a failed result.
     *
     * @param <T>       the result type
     * @param exception the exception
     * @return a failed {@link AsyncResult}
     */
    public static <T> AsyncResult<T> ofFailed(Throwable exception) {
        return new AsyncResult<>(null, exception);
    }

}
