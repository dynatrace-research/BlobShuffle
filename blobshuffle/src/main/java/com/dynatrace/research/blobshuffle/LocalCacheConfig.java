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
package com.dynatrace.research.blobshuffle;

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
