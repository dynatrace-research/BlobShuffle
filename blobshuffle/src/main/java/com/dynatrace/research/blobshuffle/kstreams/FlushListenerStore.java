package com.dynatrace.research.blobshuffle.kstreams;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.internals.CachedStateStore;
import org.apache.kafka.streams.state.internals.WrappedStateStore;

import java.util.List;

/**
 * A dummy Kafka Streams key-value state store whose sole purpose is to intercept
 * {@link #flushCache()} calls from the Kafka Streams commit machinery and forward
 * them to the {@link ProcessorFlushStoreRegistry}.
 *
 * <p>All key-value operations throw {@link UnsupportedOperationException} because
 * no actual data is stored here.
 */
public class FlushListenerStore
        extends WrappedStateStore<KeyValueStore<Bytes, byte[]>, byte[], byte[]>
        implements KeyValueStore<Bytes, byte[]>, CachedStateStore<byte[], byte[]> {

    private final ProcessorFlushStoreRegistry processorFlushStoreRegistry;

    private TaskId taskId; // ~~Effectively~~ final

    public FlushListenerStore(KeyValueStore<Bytes, byte[]> wrapped, ProcessorFlushStoreRegistry processorFlushStoreRegistry) {
        super(wrapped);
        this.processorFlushStoreRegistry = processorFlushStoreRegistry;
    }

    @Override
    public void init(StateStoreContext context, StateStore root) {
        super.init(context, root);
        this.taskId = context.taskId();
    }

    @Override
    public void flushCache() {
        // Run flush callback
        processorFlushStoreRegistry.getCallbackForTaskId(taskId).run();
    }

    @Override
    public void put(Bytes key, byte[] value) {
        throw new UnsupportedOperationException("");
    }

    @Override
    public byte[] putIfAbsent(Bytes key, byte[] value) {
        throw new UnsupportedOperationException("");
    }

    @Override
    public void putAll(List<KeyValue<Bytes, byte[]>> entries) {
        throw new UnsupportedOperationException("");
    }

    @Override
    public byte[] delete(Bytes key) {
        throw new UnsupportedOperationException("");
    }

    @Override
    public byte[] get(Bytes key) {
        throw new UnsupportedOperationException("");
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> range(Bytes from, Bytes to) {
        throw new UnsupportedOperationException("");
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> all() {
        throw new UnsupportedOperationException("");
    }

    @Override
    public long approximateNumEntries() {
        throw new UnsupportedOperationException("");
    }
}
