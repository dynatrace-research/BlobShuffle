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
package com.dynatrace.research.s3shuffler.kstreams;

import com.dynatrace.research.s3shuffler.BatchReader;
import com.dynatrace.research.s3shuffler.S3BatchFacade;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.api.*;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class S3DebatcherProcessor<K, V> extends AbstractCommitListenerProcessor<NotificationDestination, NotificationDetails, K, V> {

    private static final String DUMMY_TOPIC = "";

    private static final Logger LOGGER = LoggerFactory.getLogger(S3DebatcherProcessor.class);

    private final S3BatchFacade s3BatchFacade;

    private final Deserializer<K> keySerde;

    private final Deserializer<V> valueSerde;

    public static final AtomicInteger FORWARD_COUNTER = new AtomicInteger(); // TODO TEMP DEBUG

    public S3DebatcherProcessor(S3BatchFacade s3BatchFacade, Serde<K> keySerde, Serde<V> valueSerde, ProcessorFlushStoreRegistry processorFlushStoreRegistry) {
        super(processorFlushStoreRegistry);
        this.s3BatchFacade = s3BatchFacade;
        this.keySerde = keySerde.deserializer();
        this.valueSerde = valueSerde.deserializer();
    }

    @Override
    public void init(ProcessorContext<K, V> context) {
        super.init(context);
    }

    @Override
    public void process(Record<NotificationDestination, NotificationDetails> notification) {
        // Before processing the new record, we first collect the results of all async calls that have finished in the meantime
        this.collectAsyncResults();


        String objectPath = notification.value().objectPath();
        int offsetStart = notification.value().offsetStartInclusive();
        int offsetEnd = notification.value().offsetEndExclusive();
        this.readBatchAsync(objectPath, offsetStart, offsetEnd);
    }

    @Override
    public void close() {
        super.close();
        // TODO
    }

    @Override
    public void preCommit() { // TODO maybe protected
        System.out.println("Started commit process in debatcher. Have to wait for all pending S3 fetches.");

        System.out.println("Collect all async results now.");
        this.drainAsyncResults(); // Might block if the results to be collected
        System.out.println("All results have been collected and output records should have been forwarded.");
    }

    private final ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();
    //private final ExecutorService executorService = Executors.newFixedThreadPool(10);


    private final BlockingQueue<AsyncResult<BatchReader>> finishedAsyncCalls = new LinkedBlockingQueue<>();

    //private final Map<String, Object> pendingEvents = new HashMap<>();
    private int pendingEvents = 0;

    // TODO move up this file

    // Per task
    private final int maxInFlightWritesPerTask = 6;

    // Per task
    private final Semaphore inFlights = new Semaphore(maxInFlightWritesPerTask);

    private void readBatchAsync(String objectPath, int offsetStart, int offsetEnd) {

        // Block if too many in-flight calls
        try {
            inFlights.acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        //this.pendingEvents.put(objectPath, new Object());
        pendingEvents++;

        LOGGER.info("Start async fetching batch from object {} for offset range {}:{}.", objectPath, offsetStart, offsetEnd);

        // Perform actual send
        CompletableFuture.supplyAsync(() -> {
                    long startTime = System.currentTimeMillis(); // Use monitoring framework
                    // Blocking call!
                    InputStream batchInputStream = this.s3BatchFacade.readBatch(objectPath);
                    BatchReader batchReader = new BatchReader(batchInputStream, offsetStart, offsetEnd - offsetStart);
                    long timeMs = System.currentTimeMillis() - startTime;
                    LOGGER.info("Read batch from object {} in {} ms.", objectPath, timeMs);
                    return AsyncResult.of(batchReader);
                }, this.executorService)
                .exceptionally(AsyncResult::ofFailed)
                .whenComplete((batch, throwable) -> {
                    if (throwable != null) {
                        // This should never happen
                        LOGGER.error("Uncaught exception.", throwable);
                    }
                    this.finishedAsyncCalls.add(batch);
                    inFlights.release();
                });
    }

    private void collectAsyncResults() {
        //System.out.println("Collect all results available so far.");
        while (!this.finishedAsyncCalls.isEmpty()) {
            AsyncResult<BatchReader> asyncResult = this.finishedAsyncCalls.poll();
            if (asyncResult.isFailed()) {
                System.out.println("Received a failed result in collectAsyncResults. Rethrow exception!");
                // TODO Make custom exception
                throw new RuntimeException(asyncResult.getException());
            }
            BatchReader batchReader = asyncResult.getResult();
            //pendingEvents.remove(batchReader.getObjectPath()); // No safe in multithreaded execution, but not intended
            pendingEvents--;
            forwardBatchRecords(batchReader);
        }
    }

    private void drainAsyncResults() {
        System.out.println("Block for all results to be available.");
        // Call only after all calls have been scheduled
        //while (!this.pendingEvents.isEmpty()) {
        System.out.println("We wait for " + this.pendingEvents + " pending events.");
        while (this.pendingEvents != 0) {
            // Process one result in each iteration
            // Block if necessary
            AsyncResult<BatchReader> asyncResult = null;
            try {
                asyncResult = this.finishedAsyncCalls.take();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            if (asyncResult.isFailed()) {
                System.out.println("Received a failed result in drainAsyncResults. Rethrow exception!");
                // TODO Make custom exception
                throw new RuntimeException(asyncResult.getException());
            }
            try (BatchReader batchReader = asyncResult.getResult()) {
                //pendingEvents.remove(batchReader.getObjectPath()); // No safe in multithreaded execution, but not intended
                pendingEvents--;
                forwardBatchRecords(batchReader);
            }
        }
    }

    private void forwardBatchRecords(BatchReader batchReader) {
        while (batchReader.hasNextRecord()) {
            int readPartition = batchReader.readRecordPartition();
            int partition = context().taskId().partition();
            if (readPartition != partition) {
                // This should never happen
                throw new RuntimeException("Received record for wrong partition.");
            }
            byte[] keyBytes = batchReader.readRecordKey();
            K key = this.keySerde.deserialize(DUMMY_TOPIC, keyBytes);
            V value = this.valueSerde.deserialize(DUMMY_TOPIC, batchReader.readRecordValue());
            long timestamp = batchReader.readRecordTimestamp();
            List<Header> headers = batchReader.readHeaders();
            Record<K, V> record = new Record<>(key, value, timestamp, new RecordHeaders(headers));
            //System.out.println(record);
            FORWARD_COUNTER.incrementAndGet(); //TODO remove
            context().forward(record);
        }
    }

    public static class Supplier<K, V> extends AbstractCommitListenerProcessorSupplier<NotificationDestination, NotificationDetails, K, V> {

        private final S3BatchFacade s3BatchFacade;

        private final Serde<K> keySerde;

        private final Serde<V> valueSerde;

        public Supplier(S3BatchFacade s3BatchFacade, Serde<K> keySerde, Serde<V> valueSerde) {
            super("DUMMY-DEBATCH");
            this.s3BatchFacade = s3BatchFacade;
            this.keySerde = keySerde;
            this.valueSerde = valueSerde;
        }

        @Override
        public AbstractCommitListenerProcessor<NotificationDestination, NotificationDetails, K, V> get(ProcessorFlushStoreRegistry processorFlushStoreRegistry) {
            return new S3DebatcherProcessor<>(
                    this.s3BatchFacade,
                    this.keySerde,
                    this.valueSerde,
                    processorFlushStoreRegistry);
        }

    }

}
