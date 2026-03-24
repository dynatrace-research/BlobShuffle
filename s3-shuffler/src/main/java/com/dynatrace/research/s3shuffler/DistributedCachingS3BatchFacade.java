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
 * {@link S3BatchFacade} composition that adds per-AZ distributed caching using an HTTP
 * proxy and consistent hashing to route requests to the responsible cache node.
 */
public class DistributedCachingS3BatchFacade implements S3BatchFacade {

    private static final Logger LOGGER = LoggerFactory.getLogger(DistributedCachingS3BatchFacade.class);

    private final S3BatchFacade wrapped;

    //private final MeterRegistry metricRegistry;
    //private final Counter cacheHitsCounter;
    //private final Counter cacheMissesCounter;
    private final Timer proxyReadTimer;

    /**
     * Creates a new distributed caching facade.
     *
     * @param wrapped the underlying facade
     * @param zone the availability zone of this instance
     * @param advertisedHost the host advertised to other cache cluster members
     * @param port the port for the cache HTTP server
     * @param cacheSize the maximum number of cached entries
     * @param cacheOnWrite whether to cache batches on write (write-through)
     * @param meterRegistry the meter registry for metrics
     */
    public DistributedCachingS3BatchFacade(
            S3BatchFacade wrapped,
            String zone,
            String advertisedHost,
            int port,
            int cacheSize,
            boolean cacheOnWrite,
            MeterRegistry meterRegistry) {
        /*
        this.wrapped = new ProxyingS3BatchFacade(
                new CachingS3BatchFacade(
                        wrapped,
                        cacheSize,
                        cacheOnWrite,
                        meterRegistry,
                        "distributed"),
                zone,
                advertisedHost,
                port,
                meterRegistry);
        */
        this.wrapped = new ProxyingS3BatchFacade(
                new FullBatchS3BatchFacade(
                        new CachingS3BatchFacade(
                                wrapped,
                                cacheSize,
                                cacheOnWrite,
                                meterRegistry,
                                "distributed")
                ),
                zone,
                advertisedHost,
                port,
                meterRegistry);


        /*
        this.metricRegistry = meterRegistry;
        this.cacheHitsCounter = Counter
                .builder("s3.cache.distributed.hits")
                .description("number of hits on the distributed cache")
                .register(meterRegistry);
        this.cacheMissesCounter = Counter
                .builder("s3.cache.distributed.misses")
                .description("number of misses on the distributed cache")
                .register(meterRegistry);
               */
        this.proxyReadTimer = Timer.builder("s3.cache.proxy.get.latency")
                .description("time for getting an object via the cache proxy") // optional
                //.tags("region", "test") // optional
                .publishPercentiles(0.5, 0.95, 0.99, 0.999) // median and 95th, 99th and 99.9th percentile
                .publishPercentileHistogram()
                //.serviceLevelObjectives(Duration.ofMillis(100))
                .minimumExpectedValue(Duration.ofMillis(1))
                .maximumExpectedValue(Duration.ofSeconds(60))
                .register(meterRegistry);

    }

    @Override
    public void writeBatch(String objectPath, InputStream batchInputStream, int batchSizeInBytes) {
        this.wrapped.writeBatch(objectPath, batchInputStream, batchSizeInBytes);
    }

    @Override
    public SizeAwareInputStream readBatch(String objectPath) {
        return this.proxyReadTimer.record(() -> this.wrapped.readBatch(objectPath));
    }

    @Override
    public SizeAwareInputStream readPartialBatch(String objectPath, int offsetStart, int offsetEnd) {
        return this.proxyReadTimer.record(() -> this.wrapped.readPartialBatch(objectPath, offsetStart, offsetEnd));
    }

    @Override
    public void close() {
        this.wrapped.close();
    }
}
