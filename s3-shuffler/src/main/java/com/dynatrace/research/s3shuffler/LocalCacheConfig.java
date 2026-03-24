package com.dynatrace.research.s3shuffler;

import io.micrometer.core.instrument.MeterRegistry;

/**
 * Configuration for the optional per-instance local LRU cache.
 *
 * @param enabled whether the local cache is active
 * @param sizeInBytes maximum cache size in bytes
 * @param cacheOnWrite whether to cache batches on write (write-through)
 */
public record LocalCacheConfig(
        boolean enabled,
        long sizeInBytes,
        boolean cacheOnWrite) {

    /**
     * Optionally wraps the given {@link S3BatchFacade} with a local caching layer ({@link LocalCachingS3BatchFacade})
     * based on this {@link DistributedCacheConfig}.
     *
     * @param s3BatchFacade the facade to wrap
     * @param batchDesiredBytes the desired batch size in bytes, used to compute the cache capacity
     * @param meterRegistry the meter registry for metrics
     * @return the original facade, or a wrapped facade if the local cache is enabled
     */
    public S3BatchFacade maybeWrapS3BatchFacade(S3BatchFacade s3BatchFacade, long batchDesiredBytes, MeterRegistry meterRegistry) {
        if (this.enabled()) {
            return new LocalCachingS3BatchFacade(
                    s3BatchFacade,
                    (int) (this.sizeInBytes() / batchDesiredBytes),
                    this.cacheOnWrite,
                    meterRegistry);
        } else {
            return s3BatchFacade;
        }
    }

    /**
     * Returns a configuration with the local cache disabled.
     *
     * @return a disabled local cache configuration
     */
    public static LocalCacheConfig disabled() {
        return new LocalCacheConfig(false, 0, false);
    }

    /**
     * Returns a configuration with the local cache enabled.
     *
     * @param sizeInBytes maximum cache size in bytes
     * @param cacheOnWrite whether to cache batches on write
     * @return an enabled local cache configuration
     */
    public static LocalCacheConfig enabled(long sizeInBytes, boolean cacheOnWrite) {
        return new LocalCacheConfig(true, sizeInBytes, cacheOnWrite);
    }

}
