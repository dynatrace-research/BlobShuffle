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
package com.dynatrace.research.blobshuffle;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Simple in-memory LRU cache for byte array objects, backed by a synchronized
 * access-order {@link LinkedHashMap}.
 */
public class InMemoryBlobCache implements BlobCache {

    private final Map<String, byte[]> cache;

    /**
     * Creates a new LRU cache with the given maximum number of entries.
     *
     * @param maxCacheSize the maximum number of entries before eviction
     */
    public InMemoryBlobCache(final int maxCacheSize) {
        // TODO Consider implementing a more efficient concurrent LinkedHashMap, see:
        // https://stackoverflow.com/questions/2698217/implementing-a-concurrent-linkedhashmap
        // Partially copied from Kafka Streams: MemoryLRUCache
        // leave room for one extra entry to handle adding an entry before the oldest can be removed
        this.cache = Collections.synchronizedMap(
                new LinkedHashMap<>(maxCacheSize + 1, 1.01f, true) {

                    @Override
                    protected boolean removeEldestEntry(final Map.Entry<String, byte[]> eldest) {
                        final boolean evict = super.size() > maxCacheSize;
                        // Could do notify here
                        return evict;
                    }
                });
    }

    @Override
    public byte[] getObject(String objectPath) {
        return cache.get(objectPath);
    }

    @Override
    public void putObject(String objectPath, byte[] object) {
        cache.put(objectPath, object);
    }

}
