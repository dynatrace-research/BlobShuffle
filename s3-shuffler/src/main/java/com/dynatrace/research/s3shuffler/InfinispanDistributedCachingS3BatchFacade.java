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

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.infinispan.Cache;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.manager.DefaultCacheManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

// implements S3BatchFacade
@Deprecated
public class InfinispanDistributedCachingS3BatchFacade {

    private static final Logger LOGGER = LoggerFactory.getLogger(InfinispanDistributedCachingS3BatchFacade.class);

    private static final Duration CACHE_GET_BACKOFF = Duration.ofMillis(100);

    private final BasicS3BatchFacade wrapped;

    private final boolean cacheOnWrite;

    private final Cache<String, byte[]> cache;

    private final MeterRegistry metricRegistry;
    private final Counter cacheHitsCounter;
    private final Counter cacheMissesCounter;
    private final Timer cacheReadTimer;

    public InfinispanDistributedCachingS3BatchFacade(BasicS3BatchFacade wrapped, String zone, int cacheSize, boolean cacheOnWrite, MeterRegistry meterRegistry) {
        this.wrapped = wrapped;
        this.cacheOnWrite = cacheOnWrite;

        GlobalConfigurationBuilder globalConfig = GlobalConfigurationBuilder.defaultClusteredBuilder();
        globalConfig.transport()
                .clusterName("s3shuffler-" + zone)
                .addProperty("configurationFile", "jgroups-kubernetes.xml");
        DefaultCacheManager cacheManager = new DefaultCacheManager(globalConfig.build());
        Configuration cacheConfig = new ConfigurationBuilder()
                .clustering()
                    .cacheMode(CacheMode.DIST_SYNC) // Distributed cache with synchronous replication
                    .hash()
                    .numOwners(1)
                    //.numOwners(2) // Number of nodes that hold a copy of each entry
                .memory()
                    .storage(StorageType.HEAP)
                    //.storage(StorageType.OFF_HEAP)
                    .maxCount(cacheSize)
                    .whenFull(EvictionStrategy.REMOVE)
                .build();
        System.out.println("Infinispan cache maxCount: " + cacheSize);

        this.cache = cacheManager.administration()
                .withFlags(CacheContainerAdmin.AdminFlag.VOLATILE)
                .getOrCreateCache("distributed-cache", cacheConfig);

        System.out.println("Infinispan CacheManagerInfo:");
        System.out.println(cacheManager.getCacheManagerInfo().toJson().toPrettyString());
        System.out.println("Infinispan CacheManagerInfo clusterMembers:");
        System.out.println(cacheManager.getCacheManagerInfo().getClusterMembers());

        this.metricRegistry = meterRegistry;
        this.cacheHitsCounter = Counter
                .builder("s3.cache.distributed.hits")
                .description("number of hits on the distributed cache")
                .register(meterRegistry);
        this.cacheMissesCounter = Counter
                .builder("s3.cache.distributed.misses")
                .description("number of misses on the distributed cache")
                .register(meterRegistry);
        this.cacheReadTimer = Timer.builder("s3.cache.distributed.get.latency")
                .description("time for getting an object from the distributed cache") // optional
                //.tags("region", "test") // optional
                .publishPercentiles(0.5, 0.95, 0.99, 0.999) // median and 95th, 99th and 99.9th percentile
                .publishPercentileHistogram()
                //.serviceLevelObjectives(Duration.ofMillis(100))
                .minimumExpectedValue(Duration.ofMillis(1))
                .maximumExpectedValue(Duration.ofSeconds(60))
                .register(meterRegistry);
    }

    public void writeBatch(String objectPath, InputStream batchInputStream, int batchSizeInBytes) {
        InputStream maybeWrappedInputStream;
        if (this.cacheOnWrite) {
            byte[] readBatch = loadBatchInputStream(batchInputStream, batchSizeInBytes);
            cache.put(objectPath, readBatch); // Maybe put to cache _after_ put to S3 (?)
            System.out.println("Write to distributed cache: " + objectPath + " (" + batchSizeInBytes + " bytes)");
            maybeWrappedInputStream = new ByteArrayInputStream(readBatch);
        } else {
            maybeWrappedInputStream = batchInputStream;
        }
        System.out.println("Write to S3: " + objectPath + " (" + batchSizeInBytes + " bytes)");
        this.wrapped.writeBatch(objectPath, maybeWrappedInputStream, batchSizeInBytes);
    }

    public S3BatchFacade.SizeAwareInputStream readBatch(String objectPath) {
        return this.cacheReadTimer.record(
                () -> doReadBatch(objectPath)
        );
    }

    private S3BatchFacade.SizeAwareInputStream doReadBatch(String objectPath) {
        final byte[] lock = new byte[0];
        while (true) {
            byte[] cachedObject = this.cache.putIfAbsent(objectPath, lock, 10000, TimeUnit.MILLISECONDS);
            if (cachedObject == null) {
                // No cache entry so far, we can populate the cache
                this.cacheMissesCounter.increment();
                System.out.println("Query from S3: " + objectPath + "");
                BasicS3BatchFacade.SizeAwareInputStream batchInputStream = this.wrapped.readBatch(objectPath);
                byte[] readBatch = loadBatchInputStream(batchInputStream, batchInputStream.size());
                this.cache.put(objectPath, readBatch);
                return new S3BatchFacade.SizeAwareInputStream(new ByteArrayInputStream(readBatch), readBatch.length);
            } else if (cachedObject.length == 0) {
                // Someone else already locked this entry
                System.out.println("Wait for previous query to S3: " + objectPath + "");
                try {
                    Thread.sleep(CACHE_GET_BACKOFF);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            } else {
                // Real cache entry was present before, this is our item
                this.cacheHitsCounter.increment();
                System.out.println("Read from distributed cache: " + objectPath + " (" + cachedObject.length + " bytes)");
                return new S3BatchFacade.SizeAwareInputStream(new ByteArrayInputStream(cachedObject), cachedObject.length);
            }
        }
    }

    public S3BatchFacade.SizeAwareInputStream readPartialBatch(String objectPath, int offsetStart, int offsetEnd) {
        throw new UnsupportedOperationException("Partial batch reads are not supported");
    }

    private byte[] loadBatchInputStream(InputStream batchInputStream, int batchSizeInBytes) {
        try {
            byte[] readBatch = new byte[batchSizeInBytes];
            int readBytes = batchInputStream.readNBytes(readBatch, 0, batchSizeInBytes);
            batchInputStream.close();
            if (readBytes != batchSizeInBytes) {
                throw new RuntimeException("Read less bytes than expected to be in batch");
            }
            return readBatch;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


}
