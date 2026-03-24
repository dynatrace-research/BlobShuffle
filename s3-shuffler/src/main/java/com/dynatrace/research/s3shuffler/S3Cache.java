package com.dynatrace.research.s3shuffler;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Simple in-memory LRU cache for byte array objects, backed by a synchronized
 * access-order {@link LinkedHashMap}.
 */
public class S3Cache {

    private final Map<String, byte[]> cache;

    /**
     * Creates a new LRU cache with the given maximum number of entries.
     *
     * @param maxCacheSize the maximum number of entries before eviction
     */
    public S3Cache(final int maxCacheSize) {
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

    /**
     * Returns the cached byte array for the given object path, or {@code null} if not present.
     *
     * @param objectPath the object key
     * @return the cached bytes, or {@code null}
     */
    public byte[] getObject(String objectPath) {
        return cache.get(objectPath);
    }

    /**
     * Inserts or updates a cached object.
     *
     * @param objectPath the object key
     * @param object the byte data to cache
     */
    public void putObject(String objectPath, byte[] object) {
        cache.put(objectPath, object);
    }

}
