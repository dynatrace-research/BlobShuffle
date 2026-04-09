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
    }

    @Override
    public void preCommit() {
        LOGGER.info("Started commit process. Will finalize all batches.");
        taskSharedBatcher.finalizeBatch();
        LOGGER.info("All batches have been scheduled for transfer to S3. Now waiting for results.");
        taskSharedBatcher.drainResults(this::forwardNotifications);
        LOGGER.info("All results have been collected and notifications have been forwarded.");
    }

    private void forwardNotifications(NotificationSet notificationSet) {
        for (Record<NotificationDestination, NotificationDetails> kafkaNotification : notificationSet.asKafkaRecords()) {
            context().forward(kafkaNotification);
        }
    }

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

