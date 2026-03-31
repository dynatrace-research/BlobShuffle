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
