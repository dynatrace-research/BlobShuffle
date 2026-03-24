package com.dynatrace.research.s3shuffler;

import io.micrometer.core.instrument.MeterRegistry;
import software.amazon.awssdk.services.s3.S3Client;

public record DistributedCacheConfig(
        boolean enabled,
        long sizeInBytes,
        boolean cacheOnWrite,
        String advertisedHost,
        int port) {

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

    public static DistributedCacheConfig disabled() {
        return new DistributedCacheConfig(false, 0, false, "", -1);
    }

    public static DistributedCacheConfig enabled(long sizeInBytes, boolean cacheOnWrite, String advertisedHost, int port) {
        return new DistributedCacheConfig(true, sizeInBytes, cacheOnWrite, advertisedHost, port);
    }

}
