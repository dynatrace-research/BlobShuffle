package com.dynatrace.research.blobshuffle.kstreams;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;

import java.util.Set;

/**
 * Base {@link ProcessorSupplier} that wires up a shared {@link ProcessorFlushStoreRegistry}
 * and registers a {@link FlushListenerStore} so that Kafka Streams invokes each processor's
 * pre-commit hook at the right moment.
 *
 * @param <KIn>  the input key type
 * @param <VIn>  the input value type
 * @param <KOut> the output key type
 * @param <VOut> the output value type
 */
public abstract class AbstractCommitListenerProcessorSupplier<KIn, VIn, KOut, VOut> implements ProcessorSupplier<KIn, VIn, KOut, VOut> {

    private final ProcessorFlushStoreRegistry processorFlushStoreRegistry = new ProcessorFlushStoreRegistry();

    private final String storeName;

    public AbstractCommitListenerProcessorSupplier(String name) {
        this.storeName = name;
    }

    @Override
    public Processor<KIn, VIn, KOut, VOut> get() {
        return this.get(this.processorFlushStoreRegistry);
    }

    public abstract AbstractCommitListenerProcessor<KIn, VIn, KOut, VOut> get(ProcessorFlushStoreRegistry processorFlushStoreRegistry);

    @Override
    public Set<StoreBuilder<?>> stores() {
        final StoreBuilder<KeyValueStore<Bytes, byte[]>> storeBuilder = new FlushListenerStoreBuilder(
                this.processorFlushStoreRegistry,
                this.storeName).withLoggingDisabled();
        return Set.of(storeBuilder);
    }
}
