package com.dynatrace.research.blobshuffle.standalone;

import com.dynatrace.research.blobshuffle.*;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

/**
 * A producer that transparently batches records to S3 and sends lightweight
 * notifications through Kafka. This provides the same cost-saving S3-offloading
 * behavior as the Kafka Streams BlobShuffle, but works with the plain Kafka
 * Producer/Consumer API.
 *
 * <p>Usage:
 * <pre>
 * BlobShuffleProducer&lt;byte[], byte[]&gt; producer = new BlobShuffleProducer&lt;>(...);
 * producer.send(new ProducerRecord&lt;&gt;("my-topic", key, value));
 * producer.flush();
 * producer.close();
 * </pre>
 *
 * @param <K> the record key type
 * @param <V> the record value type
 */
public class BlobShuffleProducerImpl<K, V> implements BlobShuffleProducer<K, V> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BlobShuffleProducerImpl.class);

    private static final int MAX_PARTITION_SKEW = 8;

    private final S3BatchFacade s3BatchFacade;
    private final OutputPartitioner outputPartitioner;
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;
    private final long batchDesiredBytes;
    private final Duration batchMaxDuration;
    private final String notificationTopic;
    private final KafkaProducer<byte[], byte[]> notificationProducer;
    private final ObjectIdSupplier objectIdSupplier = new ObjectIdSupplier();

    private final ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();
    private final Map<String, BatchCollector> batchCollectors = new HashMap<>();
    private final BlockingQueue<AsyncResult<NotificationBatch>> finishedAsyncCalls = new LinkedBlockingQueue<>();
    private int pendingEvents = 0;
    private long lastCommitTimestamp = System.currentTimeMillis();

    private final int maxInFlightWrites;
    private final Semaphore inFlights;

    private final Serializer<NotificationDestination> notificationKeySerializer =
            new NotificationDestinationSerde.NotificationDestinationSerializer();
    private final Serializer<NotificationDetails> notificationValueSerializer =
            new NotificationDetailsSerde.NotificationDetailsSerializer();

    private final DistributionSummary batchSizeSummary;
    private final Timer throttleTimer;

    /**
     * @param s3BatchFacade      the S3 batch facade (may include caching decorators)
     * @param outputPartitioner  maps record keys to partitions and zones
     * @param keySerializer      serializer for record keys
     * @param valueSerializer    serializer for record values
     * @param batchDesiredBytes  target batch size in bytes before flushing to S3
     * @param batchMaxDuration   maximum time before a batch is finalized
     * @param notificationTopic  Kafka topic for notification messages
     * @param bootstrapServers   Kafka bootstrap servers
     * @param meterRegistry      meter registry for metrics
     */
    public BlobShuffleProducerImpl(
            S3BatchFacade s3BatchFacade,
            OutputPartitioner outputPartitioner,
            Serializer<K> keySerializer,
            Serializer<V> valueSerializer,
            long batchDesiredBytes,
            Duration batchMaxDuration,
            String notificationTopic,
            String bootstrapServers,
            MeterRegistry meterRegistry) {
        this(s3BatchFacade, outputPartitioner, keySerializer, valueSerializer,
                batchDesiredBytes, batchMaxDuration, notificationTopic, bootstrapServers,
                new Properties(), meterRegistry, 6);
    }

    /**
     * @param s3BatchFacade          the S3 batch facade (may include caching decorators)
     * @param outputPartitioner      maps record keys to partitions and zones
     * @param keySerializer          serializer for record keys
     * @param valueSerializer        serializer for record values
     * @param batchDesiredBytes      target batch size in bytes before flushing to S3
     * @param batchMaxDuration       maximum time before a batch is finalized
     * @param notificationTopic      Kafka topic for notification messages
     * @param bootstrapServers       Kafka bootstrap servers
     * @param additionalProperties   additional Kafka producer properties (e.g. {@code acks}, {@code linger.ms})
     * @param meterRegistry          meter registry for metrics
     */
    public BlobShuffleProducerImpl(
            S3BatchFacade s3BatchFacade,
            OutputPartitioner outputPartitioner,
            Serializer<K> keySerializer,
            Serializer<V> valueSerializer,
            long batchDesiredBytes,
            Duration batchMaxDuration,
            String notificationTopic,
            String bootstrapServers,
            Properties additionalProperties,
            MeterRegistry meterRegistry) {
        this(s3BatchFacade, outputPartitioner, keySerializer, valueSerializer,
                batchDesiredBytes, batchMaxDuration, notificationTopic, bootstrapServers,
                additionalProperties, meterRegistry, 6);
    }

    /**
     * @param s3BatchFacade      the S3 batch facade (may include caching decorators)
     * @param outputPartitioner      maps record keys to partitions and zones
     * @param keySerializer          serializer for record keys
     * @param valueSerializer        serializer for record values
     * @param batchDesiredBytes      target batch size in bytes before flushing to S3
     * @param batchMaxDuration       maximum time before a batch is finalized
     * @param notificationTopic      Kafka topic for notification messages
     * @param bootstrapServers       Kafka bootstrap servers
     * @param additionalProperties   additional Kafka producer properties
     * @param meterRegistry          meter registry for metrics
     * @param maxInFlightWrites      maximum number of concurrent S3 write operations
     */
    public BlobShuffleProducerImpl(
            S3BatchFacade s3BatchFacade,
            OutputPartitioner outputPartitioner,
            Serializer<K> keySerializer,
            Serializer<V> valueSerializer,
            long batchDesiredBytes,
            Duration batchMaxDuration,
            String notificationTopic,
            String bootstrapServers,
            Properties additionalProperties,
            MeterRegistry meterRegistry,
            int maxInFlightWrites) {
        this.s3BatchFacade = s3BatchFacade;
        this.outputPartitioner = outputPartitioner;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.batchDesiredBytes = batchDesiredBytes;
        this.batchMaxDuration = batchMaxDuration;
        this.notificationTopic = notificationTopic;
        this.maxInFlightWrites = maxInFlightWrites;
        this.inFlights = new Semaphore(maxInFlightWrites);

        // Configure the inner notification producer
        Properties notificationProps = new Properties();
        notificationProps.putAll(additionalProperties);
        notificationProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        notificationProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        notificationProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        this.notificationProducer = new KafkaProducer<>(notificationProps);

        this.batchSizeSummary = DistributionSummary
                .builder("blobshuffle.producer.batch.size")
                .baseUnit("bytes")
                .minimumExpectedValue((double) 1024 * 1024)
                .maximumExpectedValue((double) 1024 * 1024 * 1024)
                .register(meterRegistry);
        this.throttleTimer = Timer.builder("blobshuffle.producer.throttle.time")
                .description("time throttled in the producer")
                .publishPercentiles(0.5, 0.95, 0.99, 0.999)
                .publishPercentileHistogram()
                .minimumExpectedValue(Duration.ofMillis(1))
                .maximumExpectedValue(Duration.ofSeconds(60))
                .register(meterRegistry);
    }

    /**
     * Sends a record. The record is serialized and added to an internal batch collector.
     * When the batch reaches the desired size or the maximum duration has elapsed, the
     * batch is asynchronously written to S3 and notification records are sent to Kafka.
     *
     * @param record the producer record to send
     */
    public void send(ProducerRecord<K, V> record) {
        collectAsyncResults();

        byte[] recordKey = keySerializer.serialize(record.topic(), record.key());
        byte[] recordValue = valueSerializer.serialize(record.topic(), record.value());
        long recordTimestamp = record.timestamp() != null ? record.timestamp() : System.currentTimeMillis();
        Headers recordHeaders = record.headers() != null ? record.headers() : new RecordHeaders();

        int destinationPartition = record.partition() != null
                ? record.partition()
                : outputPartitioner.getPartitionForRecordKey(recordKey);
        String destinationZone = outputPartitioner.getRackForPartition(destinationPartition);

        BatchCollector batchCollector = batchCollectors.computeIfAbsent(
                destinationZone,
                d -> new BatchCollector((int) (MAX_PARTITION_SKEW * batchDesiredBytes / outputPartitioner.getNumberOfPartitions()))
        );
        batchCollector.addRecord(
                destinationPartition,
                recordKey,
                recordValue,
                recordTimestamp,
                recordHeaders
        );

        maybeFinalizeBatch();
    }

    /**
     * Flushes all pending batches to S3 and blocks until all notifications have been
     * acknowledged by Kafka.
     */
    public void flush() {
        finalizeBatch();
        drainAsyncResults();
        notificationProducer.flush();
    }

    @Override
    public void close() {
        flush();
        executorService.close();
        notificationProducer.close();
    }

    private void maybeFinalizeBatch() {
        if (batchCollectors.values().stream().anyMatch(batch -> batch.getSizeInBytes() >= batchDesiredBytes)
                || System.currentTimeMillis() - lastCommitTimestamp > batchMaxDuration.toMillis()) {
            finalizeBatch();
        }
    }

    private void finalizeBatch() {
        for (Map.Entry<String, BatchCollector> batchWithDestination : batchCollectors.entrySet()) {
            String destination = batchWithDestination.getKey();
            BatchCollector batch = batchWithDestination.getValue();
            if (batch.getSizeInBytes() == 0) {
                continue;
            }
            writeBatchAsync(destination, batch);
        }
        batchCollectors.clear();
        lastCommitTimestamp = System.currentTimeMillis();
    }

    private void writeBatchAsync(String destination, BatchCollector batch) {
        Map<Integer, BatchCollector.OffsetRange> offsetRanges = batch.getOffsetRangeForAllPartition();
        long batchTimestamp = batch.getHighestTimestamp();
        int batchSizeInBytes = batch.getSizeInBytes();
        InputStream batchInputStream = batch.getAsInputStream();
        String objectId = objectIdSupplier.get();
        String objectPath = destination + "/" + objectId;

        if (!inFlights.tryAcquire()) {
            LOGGER.info("Throttle batch writing.");
            throttleTimer.record(() -> {
                try {
                    inFlights.acquire();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            });
        }
        pendingEvents++;
        batchSizeSummary.record(batchSizeInBytes);

        CompletableFuture.supplyAsync(() -> {
            long startTime = System.currentTimeMillis();
            s3BatchFacade.writeBatch(objectPath, batchInputStream, batchSizeInBytes);
            long timeMs = System.currentTimeMillis() - startTime;
            LOGGER.info("Wrote batch of {} bytes to object {} in {} ms.", batchSizeInBytes, objectPath, timeMs);
            return AsyncResult.of(new NotificationBatch(destination, objectPath, offsetRanges, batchTimestamp));
        }, executorService)
                .exceptionally(AsyncResult::ofFailed)
                .whenComplete((result, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Uncaught exception.", throwable);
                    }
                    finishedAsyncCalls.add(result);
                    inFlights.release();
                });
    }

    private void collectAsyncResults() {
        while (!finishedAsyncCalls.isEmpty()) {
            AsyncResult<NotificationBatch> asyncResult = finishedAsyncCalls.poll();
            processAsyncResult(asyncResult);
        }
    }

    private void drainAsyncResults() {
        LOGGER.info("Block for {} pending results to be available.", pendingEvents);
        while (pendingEvents != 0) {
            AsyncResult<NotificationBatch> asyncResult;
            try {
                asyncResult = finishedAsyncCalls.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            processAsyncResult(asyncResult);
        }
    }

    private void processAsyncResult(AsyncResult<NotificationBatch> asyncResult) {
        if (asyncResult.isFailed()) {
            throw new RuntimeException(asyncResult.getException());
        }
        NotificationBatch notificationBatch = asyncResult.getResult();
        pendingEvents--;
        sendNotifications(notificationBatch);
    }

    private void sendNotifications(NotificationBatch notificationBatch) {
        for (Map.Entry<Integer, BatchCollector.OffsetRange> entry : notificationBatch.partitionsToOffsetRanges.entrySet()) {
            int partition = entry.getKey();
            BatchCollector.OffsetRange offsetRange = entry.getValue();

            NotificationDestination notifKey = new NotificationDestination(notificationBatch.destination, partition);
            NotificationDetails notifValue = new NotificationDetails(
                    notificationBatch.objectPath,
                    offsetRange.startInclusive(),
                    offsetRange.endExclusive());

            byte[] keyBytes = notificationKeySerializer.serialize(notificationTopic, notifKey);
            byte[] valueBytes = notificationValueSerializer.serialize(notificationTopic, notifValue);

            notificationProducer.send(new ProducerRecord<>(notificationTopic, partition, keyBytes, valueBytes));
        }
    }

    private record NotificationBatch(
            String destination,
            String objectPath,
            Map<Integer, BatchCollector.OffsetRange> partitionsToOffsetRanges,
            long batchTimestamp) {}
}
