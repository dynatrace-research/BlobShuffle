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
package com.dynatrace.research.blobshuffle.kstreams;

import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Manages async I/O operations with in-flight throttling and result collection.
 *
 * <p>Encapsulates the executor, completion queue, semaphore, and pending-operation counter
 * that is otherwise duplicated between {@link BatcherProcessor} and {@link DebatcherProcessor}.
 *
 * @param <T> the result type produced by each async operation
 */
public class AsyncOperationManager<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncOperationManager.class);

    private final ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();

    private final BlockingQueue<AsyncResult<T>> finishedResults = new LinkedBlockingQueue<>();

    private final Semaphore inFlights;

    private final Timer throttleTimer;

    private int pendingCount = 0;

    /**
     * @param maxInFlight   maximum number of concurrent in-flight operations
     * @param throttleTimer timer recording time spent blocked waiting for an in-flight slot
     */
    public AsyncOperationManager(int maxInFlight, Timer throttleTimer) {
        this.inFlights = new Semaphore(maxInFlight);
        this.throttleTimer = throttleTimer;
    }

    /**
     * Submits a new async operation. Blocks if the maximum number of in-flight operations has
     * been reached.
     *
     * @param operation the operation to execute asynchronously; must not return {@code null}
     */
    public void submitAsync(Supplier<T> operation) {
        if (!inFlights.tryAcquire()) {
            LOGGER.info("Throttling async submission.");
            throttleTimer.record(() -> {
                try {
                    inFlights.acquire();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            });
        }
        pendingCount++;

        CompletableFuture.supplyAsync(() -> AsyncResult.of(operation.get()), executorService)
                .exceptionally(AsyncResult::ofFailed)
                .whenComplete((result, throwable) -> {
                    if (throwable != null) {
                        // Should never happen: exceptionally() absorbs all exceptions above
                        LOGGER.error("Uncaught exception in async operation.", throwable);
                    }
                    finishedResults.add(result);
                    inFlights.release();
                });
    }

    /**
     * Processes all results that have already finished, without blocking.
     *
     * @param resultConsumer called for each completed result
     * @throws RuntimeException wrapping the original exception if any operation failed
     */
    public void collectResults(Consumer<T> resultConsumer) {
        while (!finishedResults.isEmpty()) {
            processOne(finishedResults.poll(), resultConsumer);
        }
    }

    /**
     * Blocks until all pending operations have completed and processes their results.
     *
     * @param resultConsumer called for each completed result
     * @throws RuntimeException wrapping the original exception if any operation failed
     */
    public void drainResults(Consumer<T> resultConsumer) {
        LOGGER.info("Blocking for {} pending async results.", pendingCount);
        while (pendingCount > 0) {
            try {
                processOne(finishedResults.take(), resultConsumer);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }

    private void processOne(AsyncResult<T> asyncResult, Consumer<T> resultConsumer) {
        if (asyncResult.isFailed()) {
            throw new RuntimeException(asyncResult.getException());
        }
        pendingCount--;
        resultConsumer.accept(asyncResult.getResult());
    }
}
