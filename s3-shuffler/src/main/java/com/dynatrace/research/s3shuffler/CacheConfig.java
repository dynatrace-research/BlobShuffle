package com.dynatrace.research.s3shuffler;

import io.micrometer.core.instrument.MeterRegistry;
import software.amazon.awssdk.services.s3.S3Client;

public record CacheConfig(
        boolean cacheLocalEnabled,
        long cacheLocalSizeBytes,
        boolean cacheDistributedEnabled,
        long cacheDistributedSizeBytes,
        String cacheDistributedAdvertisedHost,
        int cacheDistributedPort) {

    public S3BatchFacade toS3BatchFacade(String zone, S3Client s3Client, String bucketName, long batchDesiredBytes, MeterRegistry meterRegistry) {
        BasicS3BatchFacade s3BatchFacade = new BasicS3BatchFacade(s3Client, bucketName, meterRegistry);
        if (this.cacheDistributedEnabled() && this.cacheLocalEnabled()) {
            return new CachingS3BatchFacade( // Local cache
                    new DistributedCachingS3BatchFacade(
                            s3BatchFacade,
                            zone,
                            this.cacheDistributedAdvertisedHost,
                            this.cacheDistributedPort,
                            (int) (this.cacheDistributedSizeBytes() / batchDesiredBytes),
                            true,
                            meterRegistry),
                    (int) (this.cacheLocalSizeBytes() / batchDesiredBytes),
                    true,
                    meterRegistry,
                    "local");
        } else if (this.cacheDistributedEnabled()) {
            return new DistributedCachingS3BatchFacade(
                    s3BatchFacade,
                    zone,
                    this.cacheDistributedAdvertisedHost,
                    this.cacheDistributedPort,
                    (int) (this.cacheDistributedSizeBytes() / batchDesiredBytes),
                    true,
                    meterRegistry);
        } else if (this.cacheLocalEnabled()) {
            return new CachingS3BatchFacade(
                    s3BatchFacade,
                    (int) (this.cacheLocalSizeBytes() / batchDesiredBytes),
                    true,
                    meterRegistry,
                    "local");
        } else {
            return s3BatchFacade;
        }
    }

    public static CacheConfig noCache() {
        return new CacheConfig(false, 0, false, 0, "", -1);
    }

}
