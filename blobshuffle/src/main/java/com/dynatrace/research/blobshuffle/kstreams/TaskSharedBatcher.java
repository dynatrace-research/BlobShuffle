package com.dynatrace.research.blobshuffle.kstreams;

import com.dynatrace.research.blobshuffle.AsyncResult;
import com.dynatrace.research.blobshuffle.BatchCollector;
import com.dynatrace.research.blobshuffle.ObjectIdSupplier;
import com.dynatrace.research.blobshuffle.OutputPartitioner;
import com.dynatrace.research.blobshuffle.S3BatchFacade;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Accumulates records into per-zone batches and manages async S3 writes.
 *
 * <p>One instance is shared across all {@link BatcherProcessor} instances (i.e., all Kafka
 * Streams tasks) so that batches are filled across task boundaries, maximising S3 write
 * efficiency. Thread-safety of the shared state is deferred to a later refactoring.
 */
public class TaskSharedBatcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskSharedBatcher.class);

    // Max. tolerable difference in load on a partition vs. the avg. across partitions.
    // TODO Can be decreased if we incorporate the number of partitions per zone.
    public static final int MAX_PARTITION_SKEW = 8;

    private static final int MAX_IN_FLIGHT_WRITES = 6;

    private final S3BatchFacade s3BatchFacade;

    private final OutputPartitioner outputPartitioner;

    private final long batchDesiredBytes;

    private final Duration batchMaxDuration;

    private final ObjectIdSupplier objectIdSupplier = new ObjectIdSupplier();

    private final DistributionSummary batchSizeSummary;

    private final AsyncOperationManager<NotificationSet> asyncOps;

    private final Map<String, BatchCollector> batchCollectors = new HashMap<>();

    private long lastCommitTimestamp = System.currentTimeMillis();

    public TaskSharedBatcher(
            S3BatchFacade s3BatchFacade,
            OutputPartitioner outputPartitioner,
            long batchDesiredBytes,
            Duration batchMaxDuration,
            MeterRegistry meterRegistry) {
        this.s3BatchFacade = s3BatchFacade;
        this.outputPartitioner = outputPartitioner;
        this.batchDesiredBytes = batchDesiredBytes;
        this.batchMaxDuration = batchMaxDuration;

        Timer throttleTimer = Timer.builder("batcher.throttle.time")
                .description("time throttled in the batcher")
                .publishPercentiles(0.5, 0.95, 0.99, 0.999)
                .publishPercentileHistogram()
                .minimumExpectedValue(Duration.ofMillis(1))
                .maximumExpectedValue(Duration.ofSeconds(60))
                .register(meterRegistry);
        this.asyncOps = new AsyncOperationManager<>(MAX_IN_FLIGHT_WRITES, throttleTimer);

        this.batchSizeSummary = DistributionSummary
                .builder("batch.size")
                .baseUnit("bytes")
                .minimumExpectedValue((double) 1024 * 1024)
                .maximumExpectedValue((double) 1024 * 1024 * 1024)
                .register(meterRegistry);
    }

    /**
     * Serializes the record using the provided key/value bytes and routes it to the correct
     * per-zone {@link BatchCollector} based on the record key.
     */
    public void addRecord(byte[] recordKey, byte[] recordValue, long recordTimestamp, Headers recordHeaders) {
        int destinationPartition = outputPartitioner.getPartitionForRecordKey(recordKey);
        String destinationZone = outputPartitioner.getRackForPartition(destinationPartition);
        BatchCollector batchCollector = batchCollectors.computeIfAbsent(
                destinationZone,
                d -> new BatchCollector((int) (MAX_PARTITION_SKEW * batchDesiredBytes / outputPartitioner.getNumberOfPartitions())));
        batchCollector.addRecord(destinationPartition, recordKey, recordValue, recordTimestamp, recordHeaders);
    }

    /**
     * Finalizes the current batch if the size or time threshold has been reached.
     */
    public void maybeFinalizeBatch() {
        if (batchCollectors.values().stream().anyMatch(batch -> batch.getSizeInBytes() >= batchDesiredBytes)
                || System.currentTimeMillis() - lastCommitTimestamp > batchMaxDuration.toMillis()) {
            LOGGER.info("Finalizing batch: max size or max duration reached.");
            finalizeBatch();
        }
    }

    /**
     * Finalizes all non-empty batches and schedules async S3 writes for each.
     */
    public void finalizeBatch() {
        for (Map.Entry<String, BatchCollector> entry : batchCollectors.entrySet()) {
            String destination = entry.getKey();
            BatchCollector batch = entry.getValue();
            if (batch.getSizeInBytes() == 0) {
                continue;
            }
            writeBatchAsync(destination, batch);
        }
        batchCollectors.clear();
        lastCommitTimestamp = System.currentTimeMillis();
    }

    /**
     * Processes all results that have finished without blocking.
     *
     * @param notificationConsumer called for each completed {@link NotificationSet}
     */
    public void collectResults(Consumer<NotificationSet> notificationConsumer) {
        asyncOps.collectResults(notificationConsumer);
    }

    /**
     * Blocks until all pending S3 writes have completed and processes their results.
     *
     * @param notificationConsumer called for each completed {@link NotificationSet}
     */
    public void drainResults(Consumer<NotificationSet> notificationConsumer) {
        asyncOps.drainResults(notificationConsumer);
    }

    private void writeBatchAsync(String destination, BatchCollector batch) {
        Map<Integer, BatchCollector.OffsetRange> offsetRanges = batch.getOffsetRangeForAllPartition();
        long batchTimestamp = batch.getHighestTimestamp();
        int batchSizeInBytes = batch.getSizeInBytes();
        InputStream batchInputStream = batch.getAsInputStream(); // locks the batch; no other methods allowed after this
        String objectPath = destination + "/" + objectIdSupplier.get();

        batchSizeSummary.record(batchSizeInBytes);

        asyncOps.submitAsync(() -> {
            long startTime = System.currentTimeMillis();
            s3BatchFacade.writeBatch(objectPath, batchInputStream, batchSizeInBytes);
            LOGGER.info("Wrote batch of {} bytes to object {} in {} ms.", batchSizeInBytes, objectPath, System.currentTimeMillis() - startTime);
            return new NotificationSet(destination, objectPath, offsetRanges, batchTimestamp);
        });
    }
}
