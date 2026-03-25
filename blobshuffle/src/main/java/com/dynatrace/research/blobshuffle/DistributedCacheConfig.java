package com.dynatrace.research.blobshuffle;

import io.micrometer.core.instrument.MeterRegistry;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Configuration for the optional per-AZ distributed LRU cache.
 *
 * @param enabled whether the distributed cache is active
 * @param sizeInBytes maximum cache size in bytes per cache node
 * @param cacheOnWrite whether to cache batches on write (write-through)
 * @param advertisedHost hostname advertised to other cache cluster members
 * @param port port for the cache HTTP server
 */
public record DistributedCacheConfig(
        boolean enabled,
        long sizeInBytes,
        boolean cacheOnWrite,
        String advertisedHost,
        int port) {

    /**
     * Optionally wraps the given {@link S3BatchFacade} with a distributed caching layer
     * ({@link DistributedCachingS3BatchFacade}) based on this {@link DistributedCacheConfig}.
     *
     * @param s3BatchFacade the facade to wrap
     * @param zone the availability zone of this instance
     * @param batchDesiredBytes the desired batch size in bytes, used to compute the cache capacity
     * @param meterRegistry the meter registry for metrics
     * @return the original facade, or a wrapped facade if the distributed cache is enabled
     */
    public S3BatchFacade maybeWrapS3BatchFacade(S3BatchFacade s3BatchFacade, String zone, long batchDesiredBytes, MeterRegistry meterRegistry) {
        if (this.enabled()) {
            return new DistributedCachingS3BatchFacade(
                    s3BatchFacade,
                    zone,
                    this.advertisedHost,
                    this.port,
                    (int) (this.sizeInBytes() / batchDesiredBytes),
                    this.cacheOnWrite,
                    meterRegistry);
        } else {
            return s3BatchFacade;
        }
    }

    /**
     * Returns a configuration with the distributed cache disabled.
     *
     * @return a disabled distributed cache configuration
     */
    public static DistributedCacheConfig disabled() {
        return new DistributedCacheConfig(false, 0, false, "", -1);
    }

    /**
     * Returns a configuration with the distributed cache enabled.
     *
     * @param sizeInBytes maximum cache size in bytes per cache node
     * @param cacheOnWrite whether to cache batches on write
     * @param advertisedHost hostname advertised to other cache cluster members
     * @param port port for the cache HTTP server
     * @return an enabled distributed cache configuration
     */
    public static DistributedCacheConfig enabled(long sizeInBytes, boolean cacheOnWrite, String advertisedHost, int port) {
        return new DistributedCacheConfig(true, sizeInBytes, cacheOnWrite, advertisedHost, port);
    }

}
