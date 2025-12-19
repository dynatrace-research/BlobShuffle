package com.dynatrace.research.s3shuffler;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.time.Duration;

public class DistributedCachingS3BatchFacade implements S3BatchFacade {

    private static final Logger LOGGER = LoggerFactory.getLogger(DistributedCachingS3BatchFacade.class);

    private final S3BatchFacade wrapped;

    //private final MeterRegistry metricRegistry;
    //private final Counter cacheHitsCounter;
    //private final Counter cacheMissesCounter;
    private final Timer proxyReadTimer;

    public DistributedCachingS3BatchFacade(
            S3BatchFacade wrapped,
            String zone,
            String advertisedHost,
            int port,
            int cacheSize,
            boolean cacheOnWrite,
            MeterRegistry meterRegistry) {
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
        /*
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
        */

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

    public void writeBatch(String objectPath, InputStream batchInputStream, int batchSizeInBytes) {
        this.wrapped.writeBatch(objectPath, batchInputStream, batchSizeInBytes);
    }

    public SizeAwareInputStream readBatch(String objectPath) {
        return this.proxyReadTimer.record(() -> this.wrapped.readBatch(objectPath));
    }

}
