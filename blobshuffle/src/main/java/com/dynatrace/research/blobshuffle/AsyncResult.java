/*
 * Copyright (c) 2026 Dynatrace LLC. All rights reserved.
 *
 * This software and associated documentation files (the "Software") are being
 * made available by Dynatrace LLC for the sole purpose of illustrating the
 * implementation of certain algorithms which are published. Permission is
 * hereby granted, free of charge, to any person obtaining a copy of the
 * Software, to view and use the Software for internal, non-production,
 * non-commercial purposes only. Without limiting the foregoing, the Software
 * may not (i) be used to process live data or train, fine-tune, enrich or
 * improve any machine learning or foundation model or other artificial
 * intelligence model or system or (ii) distributed, sublicensed, modified, used
 * to provide a service, or sold either alone or as part of or in combination
 * with any other software. The Software shall at all times be considered the
 * proprietary property of Dynatrace LLC.
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
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
