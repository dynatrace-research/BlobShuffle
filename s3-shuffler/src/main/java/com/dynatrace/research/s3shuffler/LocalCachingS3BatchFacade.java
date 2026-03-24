/*
 * Copyright (c) 2026 Dynatrace LLC. All rights reserved.
 *
 * This software and associated documentation files (the "Software") are being
 * made available by Dynatrace LLC for the sole purpose of illustrating the
 * implementation of certain algorithms which are published. Permission is
 * hereby granted, free of charge, to any person obtaining a copy of the
 * Software, to view and use the Software for internal, non-production,
 * non-commercial purposes only. Without limiting the foregoing, the Software
 * may not (i) be used to process live data or train, fine-tune, enrich or
 * improve any machine learning or foundation model or other artificial
 * intelligence model or system or (ii) distributed, sublicensed, modified, used
 * to provide a service, or sold either alone or as part of or in combination
 * with any other software. The Software shall at all times be considered the
 * proprietary property of Dynatrace LLC.
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
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
