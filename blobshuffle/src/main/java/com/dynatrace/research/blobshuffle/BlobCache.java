package com.dynatrace.research.blobshuffle;

/**
 * Cache for byte array objects, keyed by object path.
 *
 * @see InMemoryBlobCache
 * @see CaffeineBlobCache
 */
public interface BlobCache {

    /**
     * Returns the cached byte array for the given object path, or {@code null} if not present.
     *
     * @param objectPath the object key
     * @return the cached bytes, or {@code null}
     */
    byte[] getObject(String objectPath);

    /**
     * Inserts or updates a cached object.
     *
     * @param objectPath the object key
     * @param object the byte data to cache
     */
    void putObject(String objectPath, byte[] object);

    /**
     * Performs any pending maintenance operations, such as flushing deferred evictions.
     * Implementations backed by asynchronous eviction (e.g. Caffeine) should override
     * this to run synchronous cleanup; the default is a no-op.
     */
    default void cleanUp() {}

}
