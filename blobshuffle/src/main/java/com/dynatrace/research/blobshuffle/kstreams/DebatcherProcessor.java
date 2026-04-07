package com.dynatrace.research.blobshuffle.kstreams;

import com.dynatrace.research.blobshuffle.AsyncResult;
import com.dynatrace.research.blobshuffle.BatchReader;
import com.dynatrace.research.blobshuffle.NotificationDestination;
import com.dynatrace.research.blobshuffle.NotificationDetails;
import com.dynatrace.research.blobshuffle.S3BatchFacade;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.processor.api.*;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class DebatcherProcessor<K, V> extends AbstractCommitListenerProcessor<NotificationDestination, NotificationDetails, K, V> {

    private static final String DUMMY_TOPIC = "";

    private static final Logger LOGGER = LoggerFactory.getLogger(DebatcherProcessor.class);

    private final S3BatchFacade s3BatchFacade;

    private final Deserializer<K> keySerde;

    private final Deserializer<V> valueSerde;

    private final Timer throttleTimer;

    private final AtomicInteger inFlightGauge;

    private final MeterRegistry meterRegistry;

    public DebatcherProcessor(S3BatchFacade s3BatchFacade, Serde<K> keySerde, Serde<V> valueSerde, MeterRegistry registry, ProcessorFlushStoreRegistry processorFlushStoreRegistry) {
        super(processorFlushStoreRegistry);
        this.s3BatchFacade = s3BatchFacade;
        this.keySerde = keySerde.deserializer();
        this.valueSerde = valueSerde.deserializer();

        this.throttleTimer = Timer.builder("debatcher.throttle.time")
                .description("time throttled in the debatcher") // optional
                //.tags("region", "test") // optional
                .publishPercentiles(0.5, 0.95, 0.99, 0.999) // median and 95th, 99th and 99.9th percentile
                .publishPercentileHistogram()
                //.serviceLevelObjectives(Duration.ofMillis(100))
                .minimumExpectedValue(Duration.ofMillis(1))
                .maximumExpectedValue(Duration.ofSeconds(60))
                .register(registry);
        this.inFlightGauge = new AtomicInteger(0);
        this.meterRegistry = registry;
    }

    @Override
    public void init(ProcessorContext<K, V> context) {
        super.init(context);
        Gauge.builder("debatcher.inflight.requests", this.inFlightGauge, AtomicInteger::doubleValue)
                .description("number of in-flight requests")
                //.tags("region", "test") // optional
                .tag("task_id", context.taskId().toString())
                .register(meterRegistry);
    }

    @Override
    public void process(Record<NotificationDestination, NotificationDetails> notification) {
        // TODO Temp
        //LOGGER.info("########## Repartitioned to {}; ts: {}; details: {}", notification.key(), notification.timestamp(), notification.value());

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
        LOGGER.info("Start commit process. Wait for all pending S3 fetches.");
        this.drainAsyncResults(); // Might block if the results to be collected
        LOGGER.info("All results have been collected and output records have been forwarded.");
    }

    private final ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();
    //private final ExecutorService executorService = Executors.newFixedThreadPool(10);


    private final BlockingQueue<AsyncResult<BatchReader>> finishedAsyncCalls = new LinkedBlockingQueue<>();

    //private final Map<String, Object> pendingEvents = new HashMap<>();
    private int pendingEvents = 0;

    // TODO move up this file

    // Per task
    private final int maxInFlightWritesPerTask = 32; //16; // * 8; // TODO Make configurable (default: 6)
    // TODO Temporary value
    // TODO static is just a very ugly hack for now

    // Per task
    private final Semaphore inFlights = new Semaphore(maxInFlightWritesPerTask); // TODO static is just a very ugly hack for now

    private void readBatchAsync(String objectPath, int offsetStart, int offsetEnd) {

        // Block if too many in-flight calls
        if (!inFlights.tryAcquire()) {
            LOGGER.info("Throttle batch reading.");
            this.throttleTimer.record(() -> {
                try {
                    inFlights.acquire();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        this.inFlightGauge.incrementAndGet();

        //this.pendingEvents.put(objectPath, new Object());
        pendingEvents++;

        LOGGER.info("Start async fetching batch from object {} for offset range {}:{}.", objectPath, offsetStart, offsetEnd);

        // Perform actual fetch
        CompletableFuture.supplyAsync(() -> {
                    long startTime = System.currentTimeMillis(); // Use monitoring framework
                    // Blocking call!
                    S3BatchFacade.SizeAwareInputStream batchInputStream = this.s3BatchFacade.readPartialBatch(objectPath, offsetStart, offsetEnd);
                    BatchReader batchReader = new BatchReader(batchInputStream, 0, batchInputStream.size());
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
                    this.inFlightGauge.decrementAndGet();
                });
    }

    private void collectAsyncResults() {
        //System.out.println("Collect all results available so far.");
        while (!this.finishedAsyncCalls.isEmpty()) {
            AsyncResult<BatchReader> asyncResult = this.finishedAsyncCalls.poll();
            if (asyncResult.isFailed()) {
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

    private void drainAsyncResults() {
        // Call only after all calls have been scheduled
        LOGGER.info("Block for {} pending results to be available.", this.pendingEvents);
        //while (!this.pendingEvents.isEmpty()) {
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
            context().forward(record);
        }
    }

    public static class Supplier<K, V> extends AbstractCommitListenerProcessorSupplier<NotificationDestination, NotificationDetails, K, V> {

        private final S3BatchFacade s3BatchFacade;

        private final Serde<K> keySerde;

        private final Serde<V> valueSerde;

        private final MeterRegistry meterRegistry;

        public Supplier(S3BatchFacade s3BatchFacade, Serde<K> keySerde, Serde<V> valueSerde, MeterRegistry meterRegistry) {
            super("DUMMY-DEBATCH");
            this.s3BatchFacade = s3BatchFacade;
            this.keySerde = keySerde;
            this.valueSerde = valueSerde;
            this.meterRegistry = meterRegistry;
        }

        @Override
        public AbstractCommitListenerProcessor<NotificationDestination, NotificationDetails, K, V> get(ProcessorFlushStoreRegistry processorFlushStoreRegistry) {
            return new DebatcherProcessor<>(
                    this.s3BatchFacade,
                    this.keySerde,
                    this.valueSerde,
                    this.meterRegistry,
                    processorFlushStoreRegistry);
        }

    }

}
