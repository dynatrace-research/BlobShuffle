package com.dynatrace.research.blobshuffle.kstreams;

import com.dynatrace.research.blobshuffle.*;
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

import java.util.function.Consumer;

/**
 * Kafka Streams processor that collects incoming records into batches for object storage.
 *
 * <p>On each {@link #process} call the record is handed to the shared {@link TaskSharedBatcher}.
 * When a size or time threshold is exceeded the batcher finalises the current batch and writes
 * it to object storage asynchronously. Notifications for each output partition are forwarded downstream
 * as Kafka records on the notification topic.
 *
 * <p>Before every Kafka Streams commit ({@link #preCommit}) all pending batches are flushed
 * and all in-flight writes are drained to ensure exactly-once semantics.
 *
 * @param <K> the input record key type
 * @param <V> the input record value type
 */
public class BatcherProcessor<K, V> extends AbstractCommitListenerProcessor<K, V, NotificationDestination, NotificationDetails> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BatcherProcessor.class);

    private static final String DUMMY_TOPIC = "";

    private final Serializer<K> keySerde;

    private final Serializer<V> valueSerde;

    private final TaskSharedBatcher taskSharedBatcher;

    private final Consumer<ProcessorContext<NotificationDestination, NotificationDetails>> onInitCallback;

    private BatcherProcessor(
            Serde<K> keySerde,
            Serde<V> valueSerde,
            TaskSharedBatcher taskSharedBatcher,
            Consumer<ProcessorContext<NotificationDestination, NotificationDetails>> onInitCallback,
            ProcessorFlushStoreRegistry processorFlushStoreRegistry) {
        super(processorFlushStoreRegistry);
        this.keySerde = keySerde.serializer();
        this.valueSerde = valueSerde.serializer();
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
        taskSharedBatcher.collectResults(this::forwardNotifications);

        byte[] recordKey = keySerde.serialize(DUMMY_TOPIC, record.key());
        byte[] recordValue = valueSerde.serialize(DUMMY_TOPIC, record.value());
        taskSharedBatcher.addRecord(recordKey, recordValue, record.timestamp(), record.headers());
        taskSharedBatcher.maybeFinalizeBatch();
    }

    @Override
    public void close() {
        super.close();
        keySerde.close();
        valueSerde.close();
    }

    @Override
    public void preCommit() {
        LOGGER.debug("Started commit process. Will finalize all batches.");
        taskSharedBatcher.finalizeBatch();
        LOGGER.debug("All batches have been scheduled for transfer to object storage. Now waiting for results.");
        taskSharedBatcher.drainResults(this::forwardNotifications);
        LOGGER.debug("All results have been collected and notifications have been forwarded.");
    }

    private void forwardNotifications(NotificationSet notificationSet) {
        for (Record<NotificationDestination, NotificationDetails> kafkaNotification : notificationSet.asKafkaRecords()) {
            context().forward(kafkaNotification);
        }
    }

    /**
     * {@link ProcessorSupplier} for {@link BatcherProcessor}.
     *
     * @param <K> the input record key type
     * @param <V> the input record value type
     */
    public static class Supplier<K, V> extends AbstractCommitListenerProcessorSupplier<K, V, NotificationDestination, NotificationDetails> {

        private final Serde<K> keySerde;

        private final Serde<V> valueSerde;

        private final TaskSharedBatcher taskSharedBatcher;

        private final Consumer<ProcessorContext<NotificationDestination, NotificationDetails>> onInitCallback;

        public Supplier(
                Serde<K> keySerde,
                Serde<V> valueSerde,
                TaskSharedBatcher taskSharedBatcher,
                Consumer<ProcessorContext<NotificationDestination, NotificationDetails>> onInitCallback) {
            super("DUMMY-BATCHER");
            this.keySerde = keySerde;
            this.valueSerde = valueSerde;
            this.taskSharedBatcher = taskSharedBatcher;
            this.onInitCallback = onInitCallback;
        }

        @Override
        public AbstractCommitListenerProcessor<K, V, NotificationDestination, NotificationDetails> get(ProcessorFlushStoreRegistry processorFlushStoreRegistry) {
            return new BatcherProcessor<>(
                    this.keySerde,
                    this.valueSerde,
                    this.taskSharedBatcher,
                    this.onInitCallback,
                    processorFlushStoreRegistry);
        }
    }
}

