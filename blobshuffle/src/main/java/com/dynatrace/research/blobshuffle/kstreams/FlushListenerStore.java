/*
 * Copyright (c) 2026 Dynatrace LLC. All rights reserved.
 *
 * This software and associated documentation files (the "Software") are being
 * made available by Dynatrace LLC for the sole purpose of illustrating the
 * implementation of certain algorithms which are published. Permission is
 * hereby granted, free of charge, to any person obtaining a copy of the
 * Software, to view and use the Software for internal, non-production,
 * non-commercial purposes only. Without limiting the foregoing, the Software
 * may not (i) be used to process live data or train, fine-tune, enrich or
 * improve any machine learning or foundation model or other artificial
 * intelligence model or system or (ii) distributed, sublicensed, modified, used
 * to provide a service, or sold either alone or as part of or in combination
 * with any other software. The Software shall at all times be considered the
 * proprietary property of Dynatrace LLC.
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
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
