package com.dynatrace.research.s3shuffler;

import io.micrometer.core.instrument.MeterRegistry;

public record LocalCacheConfig(
        boolean enabled,
        long sizeInBytes,
        boolean cacheOnWrite) {

    public S3BatchFacade maybeWrapS3BatchFacade(S3BatchFacade s3BatchFacade, long batchDesiredBytes, MeterRegistry meterRegistry) {
        if (this.enabled()) {
            return new CachingS3BatchFacade(
                    s3BatchFacade,
                    (int) (this.sizeInBytes() / batchDesiredBytes),
                    this.cacheOnWrite,
                    meterRegistry,
                    "local");
        } else {
            return s3BatchFacade;
        }
    }

    public static LocalCacheConfig noCache() {
        return new LocalCacheConfig(false, 0, false);
    }

}
