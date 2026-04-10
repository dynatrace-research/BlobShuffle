package com.dynatrace.research.blobshuffle.kstreams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.AbstractStoreBuilder;

/**
 * {@link org.apache.kafka.streams.state.StoreBuilder} that creates a {@link FlushListenerStore}.
 * The store is backed by an in-memory key-value store but delegates flush events to
 * the provided {@link ProcessorFlushStoreRegistry}.
 */
public class FlushListenerStoreBuilder extends AbstractStoreBuilder<Bytes, byte[], KeyValueStore<Bytes, byte[]>> {

    private final KeyValueBytesStoreSupplier storeSupplier;

    private final ProcessorFlushStoreRegistry processorFlushStoreRegistry;

    /**
     * Creates a new builder.
     *
     * @param flushStoreRegistry the registry that receives flush callbacks
     * @param name               the store name (must be unique within the topology)
     */
    public FlushListenerStoreBuilder(ProcessorFlushStoreRegistry flushStoreRegistry, String name) {
        super(name, Serdes.Bytes(), Serdes.ByteArray(), Time.SYSTEM); // Most never used
        this.storeSupplier = Stores.inMemoryKeyValueStore(name);
        this.processorFlushStoreRegistry = flushStoreRegistry;
    }

    @Override
    public KeyValueStore<Bytes, byte[]> build() {
        return new FlushListenerStore(this.storeSupplier.get(), processorFlushStoreRegistry);
    }

}
