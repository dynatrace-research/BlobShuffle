package com.dynatrace.research.blobshuffle.kstreams;

import com.dynatrace.research.blobshuffle.BatchCollector;
import com.dynatrace.research.blobshuffle.ObjectIdSupplier;
import com.dynatrace.research.blobshuffle.OutputPartitioner;
import com.dynatrace.research.blobshuffle.S3BatchFacade;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.processor.api.*;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.*;
import java.util.function.Consumer;

public class BatcherProcessor<K, V> extends AbstractCommitListenerProcessor<K, V, NotificationDestination, NotificationDetails> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BatcherProcessor.class);

    //private static final int MAX_LATENCY_MS = 5_000;

    private static final String DUMMY_TOPIC = "";

    // TODO Can be decreased if we incorporate the number of partitions per zone
    public static final int MAX_PARTITION_SKEW = 8; // Max. tolerable difference on load on a partition vs. the avg. across partitions

    private final long batchDesiredBytes;

    private final Duration batchMaxDuration;

    private final ObjectIdSupplier objectIdSupplier = new ObjectIdSupplier();

    private final OutputPartitioner outputPartitioner;

    private final S3BatchFacade s3BatchFacade;

    private final Serializer<K> keySerde;

    private final Serializer<V> valueSerde;

    private final TaskSharedBatcher taskSharedBatcher;

    private final Consumer<ProcessorContext<NotificationDestination, NotificationDetails>> onInitCallback;

    private final DistributionSummary batchSizeSummary;

    private final Timer throttleTimer;

    private BatcherProcessor(
            S3BatchFacade s3BatchFacade,
            OutputPartitioner outputPartitioner,
            Serde<K> keySerde,
            Serde<V> valueSerde,
            long batchDesiredBytes,
            Duration batchMaxDuration,
            TaskSharedBatcher taskSharedBatcher,
            Consumer<ProcessorContext<NotificationDestination, NotificationDetails>> onInitCallback,
            MeterRegistry registry,
            ProcessorFlushStoreRegistry processorFlushStoreRegistry) {
        super(processorFlushStoreRegistry);
        this.s3BatchFacade = s3BatchFacade;
        this.outputPartitioner = outputPartitioner;
        this.keySerde = keySerde.serializer();
        this.valueSerde = valueSerde.serializer();
        this.batchDesiredBytes = batchDesiredBytes;
        this.batchMaxDuration = batchMaxDuration;
        this.taskSharedBatcher = taskSharedBatcher;
        this.onInitCallback = onInitCallback;

        this.batchSizeSummary = DistributionSummary
                .builder("batch.size")
                .baseUnit("bytes") // optional
                //.tags("region", "test") // optional
                .minimumExpectedValue((double) 1024 * 1024) // 1 MB
                .maximumExpectedValue((double) 1024 * 1024 * 1024) // 1 GB
                //.scale(100) // optional
                .register(registry);
        this.throttleTimer = Timer.builder("batcher.throttle.time")
                .description("time throttled in the batcher") // optional
                //.tags("region", "test") // optional
                .publishPercentiles(0.5, 0.95, 0.99, 0.999) // median and 95th, 99th and 99.9th percentile
                .publishPercentileHistogram()
                //.serviceLevelObjectives(Duration.ofMillis(100))
                .minimumExpectedValue(Duration.ofMillis(1))
                .maximumExpectedValue(Duration.ofSeconds(60))
                .register(registry);
    }

    @Override
    public void init(ProcessorContext<NotificationDestination, NotificationDetails> context) {
        super.init(context);

        onInitCallback.accept(context());
    }

    @Override
    public void process(Record<K, V> record) {

        // Before processing the new record, we first collect the results of all async calls that have finished in the meantime
        this.collectAsyncResults();

        // Extract record parts
        byte[] recordKey = this.keySerde.serialize(DUMMY_TOPIC, record.key());
        byte[] recordValue = this.valueSerde.serialize(DUMMY_TOPIC, record.value());
        long recordTimestamp = record.timestamp();
        Headers recordHeaders = record.headers();

        // Compute destination partition
        int destinationPartition = outputPartitioner.getPartitionForRecordKey(recordKey);
        String destinationZone = outputPartitioner.getRackForPartition(destinationPartition);

        BatchCollector batchCollector = this.taskSharedBatcher.getBatchCollectors().computeIfAbsent(
                destinationZone,
                d -> new BatchCollector((int) (MAX_PARTITION_SKEW * batchDesiredBytes / this.outputPartitioner.getNumberOfPartitions()))
                // d -> new BatchCollector(10 * 1024 * 1024) // 10 MB per Partition
        );
        batchCollector.addRecord(
                destinationPartition,
                recordKey,
                recordValue,
                recordTimestamp,
                recordHeaders
        );

        this.maybeFinalizeBatch();
    }

    @Override
    public void close() {
        super.close();
        // TODO
    }

    private void maybeFinalizeBatch() {
        if (taskSharedBatcher.getBatchCollectors().values().stream().anyMatch(batch -> batch.getSizeInBytes() >= batchDesiredBytes)
                || System.currentTimeMillis() - taskSharedBatcher.getLastCommitTimestamp() > batchMaxDuration.toMillis()) {
            System.out.println("It is time to finalize a batch because of max size or latency reached.");
            System.out.println("Batch sizes: " + taskSharedBatcher.getBatchCollectors().values().stream().map(BatchCollector::getSizeInBytes).toList() + "(desired: " + batchDesiredBytes + ")");
            System.out.println("Time since last commit: " + (System.currentTimeMillis() - taskSharedBatcher.getLastCommitTimestamp()) + "(max acceptable: " + batchMaxDuration + ")");

            this.finalizeBatch();
        }
    }

    private void finalizeBatch() {
        for (Map.Entry<String, BatchCollector> batchWithDestination: taskSharedBatcher.getBatchCollectors().entrySet()) {
            String destination = batchWithDestination.getKey();
            BatchCollector batch = batchWithDestination.getValue();
            int batchSizeInBytes = batch.getSizeInBytes();
            // TODO Check num records in batch instead of bytes size. It feels saver
            if (batchSizeInBytes == 0) {
                // Empty batch: Do not publish
                break;
            }
            this.writeBatchAsync(destination, batch);
            // TODO We cannot reset the batch here, as it might be still in use during the async call!
            //batch.reset();
        }
        // TODO Instead we clear the map of batch collectors so new ones could be created
        taskSharedBatcher.getBatchCollectors().clear();
        taskSharedBatcher.setLastCommitTimestamp(System.currentTimeMillis());
    }

    // TODO move either up this file or to TaskSharedBatcher

    // Per task
    private final int maxInFlightWritesPerTask = 6;

    // Per task
    private final Semaphore inFlights = new Semaphore(maxInFlightWritesPerTask);

    private void writeBatchAsync(String destination, BatchCollector batch) {
        // Prepare batch write
        Map<Integer, BatchCollector.OffsetRange> offsetRanges = batch.getOffsetRangeForAllPartition();
        long batchTimestamp = batch.getHighestTimestamp();
        int batchSizeInBytes = batch.getSizeInBytes();
        InputStream batchInputStream = batch.getAsInputStream(); // Now we are not allowed to call other methods on batch anymore
        String objectId = objectIdSupplier.get();
        String objectPath = destination + "/" + objectId;

        //System.out.println("Will now try schedule call for writing " + objectPath);

        // Block if too many in-flight calls
        if (!inFlights.tryAcquire()) {
            LOGGER.info("Throttle batch writing.");
            this.throttleTimer.record(() -> {
                try {
                    inFlights.acquire();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        this.taskSharedBatcher.getPendingEvents().put(objectPath, new Object());
        this.taskSharedBatcher.incPendingEventsC();

        batchSizeSummary.record(batchSizeInBytes);

        // Perform actual send
        CompletableFuture.supplyAsync(() -> {
            long startTime = System.currentTimeMillis(); // Use monitoring framework
            // Blocking call!
            //System.out.println("Start making call to S3 from async thread. " + objectPath);
            s3BatchFacade.writeBatch(objectPath, batchInputStream, batchSizeInBytes);
            long timeMs = System.currentTimeMillis() - startTime;
            LOGGER.info("Wrote batch of {} bytes to object {} in {} ms.", batchSizeInBytes, objectPath, timeMs);
            return AsyncResult.of(new NotificationSet(destination, objectPath, offsetRanges, batchTimestamp));
        }, this.taskSharedBatcher.getExecutorService())
                .exceptionally(AsyncResult::ofFailed)
                .whenComplete((notificationSet, throwable) -> {
                    if (throwable != null) {
                        // This should never happen
                        LOGGER.error("Uncaught exception.", throwable);
                    }
                    this.taskSharedBatcher.getFinishedAsyncCalls().add(notificationSet);
                    inFlights.release();
                    // TODO now we would be save to reuse a previous batch
                    //System.out.println("Finished call to S3 from async thread. " + objectPath);
                });
    }

    private void collectAsyncResults() {
        //System.out.println("Collect all results available so far.");
        while (!this.taskSharedBatcher.getFinishedAsyncCalls().isEmpty()) {
            AsyncResult<NotificationSet> asyncResult = this.taskSharedBatcher.getFinishedAsyncCalls().poll();
            if (asyncResult.isFailed()) {
                // TODO Make custom exception
                throw new RuntimeException(asyncResult.getException());
            }
            NotificationSet notificationSet = asyncResult.getResult();
            this.taskSharedBatcher.getPendingEvents().remove(notificationSet.getObjectPath()); // Not safe in multithreaded execution, but not intended
            this.taskSharedBatcher.decPendingEventsC();
            forwardNotifications(notificationSet);
        }
    }

    private void drainAsyncResults() {
        // Call only after all calls have been scheduled
        LOGGER.info("Block for {} pending results to be available.", this.taskSharedBatcher.getPendingEventsC());
        //while (!this.taskSharedBatcher.getPendingEvents().isEmpty()) {
        while (this.taskSharedBatcher.getPendingEventsC() != 0) {
            // Process one result in each iteration
            // Block if necessary
            AsyncResult<NotificationSet> asyncResult = null;
            try {
                asyncResult = this.taskSharedBatcher.getFinishedAsyncCalls().take();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            if (asyncResult.isFailed()) {
                // TODO Make custom exception
                throw new RuntimeException(asyncResult.getException());
            }
            NotificationSet notificationSet = asyncResult.getResult();
            taskSharedBatcher.getPendingEvents().remove(notificationSet.getObjectPath()); // Not safe in multithreaded execution, but not intended
            taskSharedBatcher.decPendingEventsC();
            forwardNotifications(notificationSet);
        }
    }

    private void forwardNotifications(NotificationSet notificationSet) {
        for (Record<NotificationDestination, NotificationDetails> kafkaNotification : notificationSet.asKafkaRecords()) {
            // Convert to "old" String-based notification format
            /*
            Record<String, String> notification = new Record<>(
                    kafkaNotification.key().destination() + ":" + kafkaNotification.key().partition(),
                    kafkaNotification.value().objectPath() + ":" + kafkaNotification.value().offsetStartInclusive() + ":" + kafkaNotification.value().offsetEndExclusive(),
                    kafkaNotification.timestamp()
            );
            */
            context().forward(kafkaNotification);
        }
    }

    @Override
    public void preCommit() {
        System.out.println("Started commit process. Will finalize all batches.");


        //internalContext.forward(notification);
        System.out.println("Initiate finalization of all batches now.");
        this.finalizeBatch(); // Might block if there are too many in flight calls; no pending batches should be there anymore
        System.out.println("All batches have been scheduled for transfer to S3. Now wait for results.");
        this.drainAsyncResults(); // Might block if the results to be collected
        System.out.println("All results have been collected and notifications should have been forwarded.");
    }

    public static class Supplier<K, V> extends AbstractCommitListenerProcessorSupplier<K, V, NotificationDestination, NotificationDetails> {

        private final S3BatchFacade s3BatchFacade;

        private final OutputPartitioner outputPartitioner;

        private final Serde<K> keySerde;

        private final Serde<V> valueSerde;

        private final long batchDesiredBytes;

        private final Duration batchMaxDuration;

        private final TaskSharedBatcher taskSharedBatcher;

        private final MeterRegistry meterRegistry;

        private final Consumer<ProcessorContext<NotificationDestination, NotificationDetails>> onInitCallback;

        public Supplier(S3BatchFacade s3BatchFacade, OutputPartitioner outputPartitioner, Serde<K> keySerde, Serde<V> valueSerde, long batchDesiredBytes, Duration batchMaxDuration, TaskSharedBatcher taskSharedBatcher, Consumer<ProcessorContext<NotificationDestination, NotificationDetails>> onInitCallback, MeterRegistry meterRegistry) {
            super("DUMMY-BATCHER");
            this.s3BatchFacade = s3BatchFacade;
            this.outputPartitioner = outputPartitioner;
            this.keySerde = keySerde;
            this.valueSerde = valueSerde;
            this.batchDesiredBytes = batchDesiredBytes;
            this.batchMaxDuration = batchMaxDuration;
            this.taskSharedBatcher = taskSharedBatcher;
            this.onInitCallback = onInitCallback;
            this.meterRegistry = meterRegistry;
        }

        @Override
        public AbstractCommitListenerProcessor<K, V, NotificationDestination, NotificationDetails> get(ProcessorFlushStoreRegistry processorFlushStoreRegistry) {
            return new BatcherProcessor<>(
                    this.s3BatchFacade,
                    this.outputPartitioner,
                    this.keySerde,
                    this.valueSerde,
                    this.batchDesiredBytes,
                    this.batchMaxDuration,
                    this.taskSharedBatcher,
                    this.onInitCallback,
                    this.meterRegistry,
                    processorFlushStoreRegistry);
        }


    }

}
