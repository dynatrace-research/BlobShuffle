package com.dynatrace.research.s3shuffler.kstreams;

import com.dynatrace.hash4j.hashing.Hasher64;
import com.dynatrace.hash4j.hashing.Hashing;
import com.dynatrace.research.s3shuffler.BatchCollector;
import com.dynatrace.research.s3shuffler.ObjectIdSupplier;
import com.dynatrace.research.s3shuffler.OutputPartitioner;
import com.dynatrace.research.s3shuffler.S3BatchFacade;
import com.dynatrace.research.s3shuffler.util.ByteBuffers;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.api.*;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.ProcessorNode;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class S3BatcherProcessor<K, V> extends AbstractCommitListenerProcessor<K, V, NotificationDestination, NotificationDetails> {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3BatcherProcessor.class);

    //private static final int MAX_LATENCY_MS = 5_000;

    private static final String DUMMY_TOPIC = "";

    private final long batchDesiredBytes;

    private final Duration batchMaxDuration;

    private final Hasher64 hasher = Hashing.komihash4_3(0x81c1f2b68968424dL);

    private final ObjectIdSupplier objectIdSupplier = new ObjectIdSupplier();

    private final OutputPartitioner outputPartitioner;

    private final S3BatchFacade s3BatchFacade;

    private final Serializer<K> keySerde;

    private final Serializer<V> valueSerde;

    private final TaskSharedBatcher taskSharedBatcher;

    private final Consumer<ProcessorContext<NotificationDestination, NotificationDetails>> onInitCallback;

    public static final AtomicInteger COUNTER = new AtomicInteger(); // TODO TEMP DEBUG

    public static final AtomicInteger BATCH_ADD_COUNTER = new AtomicInteger(); // TODO TEMP DEBUG

    private S3BatcherProcessor(
            S3BatchFacade s3BatchFacade,
            OutputPartitioner outputPartitioner,
            Serde<K> keySerde,
            Serde<V> valueSerde,
            long batchDesiredBytes,
            Duration batchMaxDuration,
            TaskSharedBatcher taskSharedBatcher,
            Consumer<ProcessorContext<NotificationDestination, NotificationDetails>> onInitCallback,
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
    }

    @Override
    public void init(ProcessorContext<NotificationDestination, NotificationDetails> context) {
        super.init(context);

        onInitCallback.accept(context());
    }

    @Override
    public void process(Record<K, V> record) {
        //System.out.println("Got record");
        COUNTER.incrementAndGet();

        // Before processing the new record, we first collect the results of all async calls that have finished in the meantime
        this.collectAsyncResults();

        // Extract record parts
        ByteBuffer recordKey = ByteBuffers.wrapNullable(this.keySerde.serialize(DUMMY_TOPIC, record.key()));
        ByteBuffer recordValue = ByteBuffers.wrapNullable(this.valueSerde.serialize(DUMMY_TOPIC, record.value()));
        long recordTimestamp = record.timestamp();
        Headers recordHeaders = record.headers();

        // Compute destination partition
        byte[] firstFourBytes = new byte[4];
        // byte[] keyBytes = new byte[recordKey.limit()];
        recordValue.get(firstFourBytes);
        // recordKey.get(keyBytes);
        recordValue.rewind();
        // recordKey.rewind();
        int recordHash = this.hasher.hashBytesToInt(firstFourBytes, 0, firstFourBytes.length);
        // int recordHash = this.hasher.hashBytesToInt(keyBytes, 0, keyBytes.length);
        // int recordHash = Murmur3.hash32(keyBytes);
        int destinationPartition = outputPartitioner.getPartitionForRecordHash(recordHash);
        String destinationZone = outputPartitioner.getRackForPartition(destinationPartition);

        BatchCollector batchCollector = this.taskSharedBatcher.getBatchCollectors().computeIfAbsent(
                destinationZone,
                d -> new BatchCollector(10 * 1024 * 1024));
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

        System.out.println("Will now try schedule call for writing " + objectPath);

        // Block if too many in-flight calls
        try {
            if (!inFlights.tryAcquire()) {
                System.out.println("We have to wait now...");
                inFlights.acquire();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        this.taskSharedBatcher.getPendingEvents().put(objectPath, new Object());
        this.taskSharedBatcher.incPendingEventsC();

        // Perform actual send
        CompletableFuture.supplyAsync(() -> {
            long startTime = System.currentTimeMillis(); // Use monitoring framework
            // Blocking call!
            System.out.println("Start making call to S3 from async thread. " + objectPath);
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
                    System.out.println("Finished call to S3 from async thread. " + objectPath);
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
        System.out.println("Block for all results to be available.");
        // Call only after all calls have been scheduled
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
                System.out.println("Received a failed result in drainAsyncResults. Rethrow exception!");
                // TODO Make custom exception
                throw new RuntimeException(asyncResult.getException());
            }
            NotificationSet notificationSet = asyncResult.getResult();
            taskSharedBatcher.getPendingEvents().remove(notificationSet.getObjectPath()); // No safe in multithreaded execution, but not intended
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

        private final Consumer<ProcessorContext<NotificationDestination, NotificationDetails>> onInitCallback;

        public Supplier(S3BatchFacade s3BatchFacade, OutputPartitioner outputPartitioner, Serde<K> keySerde, Serde<V> valueSerde, long batchDesiredBytes, Duration batchMaxDuration, TaskSharedBatcher taskSharedBatcher, Consumer<ProcessorContext<NotificationDestination, NotificationDetails>> onInitCallback) {
            super("DUMMY-BATCHER");
            this.s3BatchFacade = s3BatchFacade;
            this.outputPartitioner = outputPartitioner;
            this.keySerde = keySerde;
            this.valueSerde = valueSerde;
            this.batchDesiredBytes = batchDesiredBytes;
            this.batchMaxDuration = batchMaxDuration;
            this.taskSharedBatcher = taskSharedBatcher;
            this.onInitCallback = onInitCallback;
        }

        @Override
        public AbstractCommitListenerProcessor<K, V, NotificationDestination, NotificationDetails> get(ProcessorFlushStoreRegistry processorFlushStoreRegistry) {
            return new S3BatcherProcessor<>(
                    this.s3BatchFacade,
                    this.outputPartitioner,
                    this.keySerde,
                    this.valueSerde,
                    this.batchDesiredBytes,
                    this.batchMaxDuration,
                    this.taskSharedBatcher,
                    this.onInitCallback,
                    processorFlushStoreRegistry);
        }


    }

}
