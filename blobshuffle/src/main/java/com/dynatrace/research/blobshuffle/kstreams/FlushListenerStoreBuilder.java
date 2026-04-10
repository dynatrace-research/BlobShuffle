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
