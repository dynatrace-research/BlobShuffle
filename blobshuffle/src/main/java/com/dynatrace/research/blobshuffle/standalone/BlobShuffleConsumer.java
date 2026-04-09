package com.dynatrace.research.blobshuffle.standalone;

import com.dynatrace.research.blobshuffle.*;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

/**
 * A consumer that reads notification records from Kafka, fetches the corresponding
 * record batches from S3, and returns the deserialized original records. This provides
 * the same cost-saving S3-offloading behavior as the Kafka Streams BlobShuffle, but
 * works with the plain Kafka Producer/Consumer API.
 *
 * <p>Usage:
 * <pre>
 * BlobShuffleConsumer&lt;byte[], byte[]&gt; consumer = new BlobShuffleConsumer&lt;&gt;(...);
 * consumer.subscribe(Collections.singletonList("my-notification-topic"));
 * while (true) {
 *     List&lt;ConsumerRecord&lt;byte[], byte[]&gt;&gt; records = consumer.poll(Duration.ofMillis(100));
 *     // process records
 *     consumer.commitSync();
 * }
 * consumer.close();
 * </pre>
 *
 * @param <K> the record key type
 * @param <V> the record value type
 */
public class BlobShuffleConsumer<K, V> implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(BlobShuffleConsumer.class);

    private static final String DUMMY_TOPIC = "";

    private final S3BatchFacade s3BatchFacade;
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;
    private final KafkaConsumer<byte[], byte[]> notificationConsumer;

    private final ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();
    private final BlockingQueue<AsyncResult<List<ConsumerRecord<K, V>>>> finishedAsyncCalls = new LinkedBlockingQueue<>();
    private int pendingEvents = 0;

    private final int maxInFlightReads;
    private final Semaphore inFlights;

    private final Deserializer<NotificationDestination> notificationKeyDeserializer =
            new NotificationDestinationSerde.NotificationDestinationDeserializer();
    private final Deserializer<NotificationDetails> notificationValueDeserializer =
            new NotificationDetailsSerde.NotificationDetailsDeserializer();

    private final Timer throttleTimer;

    /**
     * @param s3BatchFacade      the S3 batch facade (may include caching decorators)
     * @param keyDeserializer    deserializer for record keys
     * @param valueDeserializer  deserializer for record values
     * @param bootstrapServers   Kafka bootstrap servers
     * @param groupId            Kafka consumer group ID
     * @param meterRegistry      meter registry for metrics
     */
    public BlobShuffleConsumer(
            S3BatchFacade s3BatchFacade,
            Deserializer<K> keyDeserializer,
            Deserializer<V> valueDeserializer,
            String bootstrapServers,
            String groupId,
            MeterRegistry meterRegistry) {
        this(s3BatchFacade, keyDeserializer, valueDeserializer,
                bootstrapServers, groupId, new Properties(), meterRegistry, 32);
    }

    /**
     * @param s3BatchFacade        the S3 batch facade (may include caching decorators)
     * @param keyDeserializer      deserializer for record keys
     * @param valueDeserializer    deserializer for record values
     * @param bootstrapServers     Kafka bootstrap servers
     * @param groupId              Kafka consumer group ID
     * @param additionalProperties additional Kafka consumer properties (e.g. {@code auto.offset.reset})
     * @param meterRegistry        meter registry for metrics
     */
    public BlobShuffleConsumer(
            S3BatchFacade s3BatchFacade,
            Deserializer<K> keyDeserializer,
            Deserializer<V> valueDeserializer,
            String bootstrapServers,
            String groupId,
            Properties additionalProperties,
            MeterRegistry meterRegistry) {
        this(s3BatchFacade, keyDeserializer, valueDeserializer,
                bootstrapServers, groupId, additionalProperties, meterRegistry, 32);
    }

    /**
     * @param s3BatchFacade        the S3 batch facade (may include caching decorators)
     * @param keyDeserializer      deserializer for record keys
     * @param valueDeserializer    deserializer for record values
     * @param bootstrapServers     Kafka bootstrap servers
     * @param groupId              Kafka consumer group ID
     * @param additionalProperties additional Kafka consumer properties
     * @param meterRegistry        meter registry for metrics
     * @param maxInFlightReads     maximum number of concurrent S3 read operations
     */
    public BlobShuffleConsumer(
            S3BatchFacade s3BatchFacade,
            Deserializer<K> keyDeserializer,
            Deserializer<V> valueDeserializer,
            String bootstrapServers,
            String groupId,
            Properties additionalProperties,
            MeterRegistry meterRegistry,
            int maxInFlightReads) {
        this.s3BatchFacade = s3BatchFacade;
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
        this.maxInFlightReads = maxInFlightReads;
        this.inFlights = new Semaphore(maxInFlightReads);

        Properties notificationProps = new Properties();
        notificationProps.putAll(additionalProperties);
        notificationProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        notificationProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        notificationProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        notificationProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        notificationProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        this.notificationConsumer = new KafkaConsumer<>(notificationProps);

        this.throttleTimer = Timer.builder("blobshuffle.consumer.throttle.time")
                .description("time throttled in the consumer")
                .publishPercentiles(0.5, 0.95, 0.99, 0.999)
                .publishPercentileHistogram()
                .minimumExpectedValue(Duration.ofMillis(1))
                .maximumExpectedValue(Duration.ofSeconds(60))
                .register(meterRegistry);
    }

    /**
     * Subscribe to the given notification topics.
     *
     * @param topics the notification topic(s) to subscribe to
     */
    public void subscribe(Collection<String> topics) {
        notificationConsumer.subscribe(topics);
    }

    /**
     * Polls for new records. This method polls the inner Kafka consumer for notification
     * records, schedules async S3 batch fetches, and returns all deserialized records
     * from completed fetches.
     *
     * @param timeout the maximum time to block waiting for notification records
     * @return the deserialized records from completed S3 batch fetches
     */
    public List<ConsumerRecord<K, V>> poll(Duration timeout) {
        // Schedule S3 reads for new notifications
        ConsumerRecords<byte[], byte[]> notifications = notificationConsumer.poll(timeout);
        for (ConsumerRecord<byte[], byte[]> notification : notifications) {
            NotificationDetails details = notificationValueDeserializer.deserialize(
                    notification.topic(), notification.value());
            readBatchAsync(details.objectPath(), details.offsetStartInclusive(), details.offsetEndExclusive());
        }

        // Collect all completed S3 reads (if notifications were received, block until all complete)
        List<ConsumerRecord<K, V>> records = new ArrayList<>();
        if (!notifications.isEmpty()) {
            drainAsyncResults(records);
        } else {
            collectAsyncResults(records);
        }
        return records;
    }

    /**
     * Commits the current consumer offsets synchronously.
     * Should only be called after successfully processing records returned by {@link #poll(Duration)}.
     */
    public void commitSync() {
        notificationConsumer.commitSync();
    }

    /**
     * Commits the current consumer offsets asynchronously.
     */
    public void commitAsync() {
        notificationConsumer.commitAsync();
    }

    @Override
    public void close() {
        notificationConsumer.close();
        executorService.close();
    }

    private void readBatchAsync(String objectPath, int offsetStart, int offsetEnd) {
        if (!inFlights.tryAcquire()) {
            LOGGER.info("Throttle batch reading.");
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

        LOGGER.info("Start async fetching batch from object {} for offset range {}:{}.", objectPath, offsetStart, offsetEnd);

        CompletableFuture.supplyAsync(() -> {
                    long startTime = System.currentTimeMillis();
                    S3BatchFacade.SizeAwareInputStream batchInputStream =
                            s3BatchFacade.readPartialBatch(objectPath, offsetStart, offsetEnd);
                    List<ConsumerRecord<K, V>> records = new ArrayList<>();
                    try (BatchReader batchReader = new BatchReader(batchInputStream, 0, batchInputStream.size())) {
                        while (batchReader.hasNextRecord()) {
                            int partition = batchReader.readRecordPartition();
                            byte[] keyBytes = batchReader.readRecordKey();
                            K key = keyDeserializer.deserialize(DUMMY_TOPIC, keyBytes);
                            byte[] valueBytes = batchReader.readRecordValue();
                            V value = valueDeserializer.deserialize(DUMMY_TOPIC, valueBytes);
                            long timestamp = batchReader.readRecordTimestamp();
                            List<Header> headers = batchReader.readHeaders();
                            records.add(new ConsumerRecord<>(
                                    DUMMY_TOPIC,
                                    partition,
                                    -1L,
                                    timestamp,
                                    TimestampType.CREATE_TIME,
                                    keyBytes != null ? keyBytes.length : 0,
                                    valueBytes != null ? valueBytes.length : 0,
                                    key,
                                    value,
                                    new RecordHeaders(headers),
                                    Optional.empty()));
                        }
                    }
                    long timeMs = System.currentTimeMillis() - startTime;
                    LOGGER.info("Read batch from object {} in {} ms.", objectPath, timeMs);
                    return AsyncResult.of(records);
                }, executorService)
                .exceptionally(AsyncResult::ofFailed)
                .whenComplete((batch, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Uncaught exception.", throwable);
                    }
                    finishedAsyncCalls.add(batch);
                    inFlights.release();
                });
    }

    private void collectAsyncResults(List<ConsumerRecord<K, V>> records) {
        while (!finishedAsyncCalls.isEmpty()) {
            processAsyncResult(finishedAsyncCalls.poll(), records);
        }
    }

    private void drainAsyncResults(List<ConsumerRecord<K, V>> records) {
        LOGGER.info("Block for {} pending results to be available.", pendingEvents);
        while (pendingEvents != 0) {
            AsyncResult<List<ConsumerRecord<K, V>>> asyncResult;
            try {
                asyncResult = finishedAsyncCalls.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            processAsyncResult(asyncResult, records);
        }
    }

    private void processAsyncResult(AsyncResult<List<ConsumerRecord<K, V>>> asyncResult, List<ConsumerRecord<K, V>> records) {
        if (asyncResult.isFailed()) {
            throw new RuntimeException(asyncResult.getException());
        }
        pendingEvents--;
        records.addAll(asyncResult.getResult());
    }
}
