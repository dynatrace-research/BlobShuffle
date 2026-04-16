package com.dynatrace.research.blobshuffle;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

/**
 * LRU cache for byte array objects backed by <a href="https://github.com/ben-manes/caffeine">Caffeine</a>.
 */
public class CaffeineBlobCache implements BlobCache {

    private final Cache<String, byte[]> cache;

    /**
     * Creates a new LRU cache with the given maximum number of entries.
     *
     * @param maxCacheSize the maximum number of entries before eviction
     */
    public CaffeineBlobCache(final int maxCacheSize) {
        this.cache = Caffeine.newBuilder()
                .maximumSize(maxCacheSize)
                .build();
    }

    @Override
    public byte[] getObject(String objectPath) {
        return cache.getIfPresent(objectPath);
    }

    @Override
    public void putObject(String objectPath, byte[] object) {
        cache.put(objectPath, object);
    }

    @Override
    public void cleanUp() {
        cache.cleanUp();
    }

}
