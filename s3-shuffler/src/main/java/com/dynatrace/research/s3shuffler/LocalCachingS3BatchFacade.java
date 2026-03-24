package com.dynatrace.research.s3shuffler;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.time.Duration;

/**
 * {@link S3BatchFacade} composition that adds a per-instance local LRU cache in front of
 * the underlying facade.
 */
public class LocalCachingS3BatchFacade implements S3BatchFacade {

    private final S3BatchFacade wrapped;

    /**
     * Creates a new local caching facade.
     *
     * @param wrapped the underlying facade
     * @param cacheSize the maximum number of cached entries
     * @param cacheOnWrite whether to cache batches on write (write-through)
     * @param meterRegistry the meter registry for metrics
     */
    public LocalCachingS3BatchFacade(
            S3BatchFacade wrapped,
            int cacheSize,
            boolean cacheOnWrite,
            MeterRegistry meterRegistry) {

        this.wrapped = new FullBatchS3BatchFacade(
                new CachingS3BatchFacade(wrapped, cacheSize, cacheOnWrite, meterRegistry, "local"));
    }

    @Override
    public void writeBatch(String objectPath, InputStream batchInputStream, int batchSizeInBytes) {
        this.wrapped.writeBatch(objectPath, batchInputStream, batchSizeInBytes);
    }

    @Override
    public SizeAwareInputStream readBatch(String objectPath) {
        return this.wrapped.readBatch(objectPath);
    }

    @Override
    public SizeAwareInputStream readPartialBatch(String objectPath, int offsetStart, int offsetEnd) {
        return this.wrapped.readPartialBatch(objectPath, offsetStart, offsetEnd);
    }

    @Override
    public void close() {
        this.wrapped.close();
    }
}
