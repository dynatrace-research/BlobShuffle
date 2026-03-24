package com.dynatrace.research.s3shuffler;

import io.micrometer.core.instrument.MeterRegistry;

public record LocalCacheConfig(
        boolean enabled,
        long sizeInBytes,
        boolean cacheOnWrite) {

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

    public static LocalCacheConfig disabled() {
        return new LocalCacheConfig(false, 0, false);
    }

    public static LocalCacheConfig enabled(long sizeInBytes, boolean cacheOnWrite) {
        return new LocalCacheConfig(true, sizeInBytes, cacheOnWrite);
    }

}
