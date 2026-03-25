package com.dynatrace.research.blobshuffle.kstreams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.AbstractStoreBuilder;

public class FlushListenerStoreBuilder extends AbstractStoreBuilder<Bytes, byte[], KeyValueStore<Bytes, byte[]>> {

    private final KeyValueBytesStoreSupplier storeSupplier;

    private final ProcessorFlushStoreRegistry processorFlushStoreRegistry;

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
