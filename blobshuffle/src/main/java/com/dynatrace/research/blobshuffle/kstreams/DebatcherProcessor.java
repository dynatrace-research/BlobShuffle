package com.dynatrace.research.blobshuffle.kstreams;

import com.dynatrace.research.blobshuffle.*;
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
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Kafka Streams processor that reads batches from object storage and re-emits the contained records.
 *
 * <p>Each incoming notification ({@link NotificationDetails}) identifies an object and a
 * byte range to fetch. The fetch is issued asynchronously; previously completed results are
 * drained on every {@link #process} call. Before every Kafka Streams commit ({@link #preCommit})
 * all in-flight reads are drained so that all records are forwarded before the offsets are
 * committed.
 *
 * @param <K> the output record key type
 * @param <V> the output record value type
 */
public class DebatcherProcessor<K, V> extends AbstractCommitListenerProcessor<NotificationDestination, NotificationDetails, K, V> {

    private static final String DUMMY_TOPIC = "";

    private static final Logger LOGGER = LoggerFactory.getLogger(DebatcherProcessor.class);

    private static final int MAX_IN_FLIGHT_READS = 32;

    private final S3BatchFacade s3BatchFacade;

    private final Deserializer<K> keySerde;

    private final Deserializer<V> valueSerde;

    private final AtomicInteger inFlightGauge;

    private final MeterRegistry meterRegistry;

    private final AsyncOperationManager<BatchReader> asyncOps;

    public DebatcherProcessor(S3BatchFacade s3BatchFacade, Serde<K> keySerde, Serde<V> valueSerde, MeterRegistry registry, ProcessorFlushStoreRegistry processorFlushStoreRegistry) {
        super(processorFlushStoreRegistry);
        this.s3BatchFacade = s3BatchFacade;
        this.keySerde = keySerde.deserializer();
        this.valueSerde = valueSerde.deserializer();

        Timer throttleTimer = Timer.builder("debatcher.throttle.time")
                .description("time throttled in the debatcher")
                .publishPercentiles(0.5, 0.95, 0.99, 0.999)
                .publishPercentileHistogram()
                .minimumExpectedValue(Duration.ofMillis(1))
                .maximumExpectedValue(Duration.ofSeconds(60))
                .register(registry);
        this.asyncOps = new AsyncOperationManager<>(MAX_IN_FLIGHT_READS, throttleTimer);
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
        // Before processing the new record, we first collect the results of all async calls that have finished in the meantime
        asyncOps.collectResults(this::forwardBatchRecords);

        String objectPath = notification.value().objectPath();
        int offsetStart = notification.value().offsetStartInclusive();
        int offsetEnd = notification.value().offsetEndExclusive();
        readBatchAsync(objectPath, offsetStart, offsetEnd);
    }

    @Override
    public void close() {
        super.close();
        keySerde.close();
        valueSerde.close();
    }

    @Override
    public void preCommit() { // TODO maybe protected
        LOGGER.debug("Start commit process. Wait for all pending S3 fetches.");
        asyncOps.drainResults(this::forwardBatchRecords); // Might block if the results to be collected
        LOGGER.debug("All results have been collected and output records have been forwarded.");
    }

    private void readBatchAsync(String objectPath, int offsetStart, int offsetEnd) {
        LOGGER.debug("Start async fetching batch from object {} for offset range {}:{}.", objectPath, offsetStart, offsetEnd);
        inFlightGauge.incrementAndGet();
        asyncOps.submitAsync(() -> {
            long startTime = System.currentTimeMillis();  // Use monitoring framework?
            // Blocking call!
            S3BatchFacade.SizeAwareInputStream batchInputStream = s3BatchFacade.readPartialBatch(objectPath, offsetStart, offsetEnd);
            BatchReader batchReader = new BatchReader(batchInputStream, 0, batchInputStream.size());
            LOGGER.debug("Read batch from object {} in {} ms.", objectPath, System.currentTimeMillis() - startTime);
            inFlightGauge.decrementAndGet();
            return batchReader;
        });
    }

    private void forwardBatchRecords(BatchReader batchReader) {
        try (batchReader) {
            while (batchReader.hasNextRecord()) {
                int readPartition = batchReader.readRecordPartition();
                int partition = context().taskId().partition();
                if (readPartition != partition) {
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
    }

    /**
     * {@link ProcessorSupplier} for {@link DebatcherProcessor}.
     *
     * @param <K> the output record key type
     * @param <V> the output record value type
     */
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
