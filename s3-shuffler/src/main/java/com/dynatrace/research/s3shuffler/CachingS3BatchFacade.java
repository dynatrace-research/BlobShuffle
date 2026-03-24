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
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

/**
 * {@link S3BatchFacade} decorator that adds in-memory LRU caching with coordination
 * to avoid redundant downloads for concurrent reads of the same batch.
 */
public class CachingS3BatchFacade implements S3BatchFacade {

    private static final Logger LOGGER = LoggerFactory.getLogger(CachingS3BatchFacade.class);

    private final S3BatchFacade wrapped;

    private final boolean cacheOnWrite;

    private final S3Cache s3Cache;

    private final ConcurrentHashMap<String, Object> everCached = new ConcurrentHashMap<>(); // TODO Debug: track all ever seen objects

    private static final Object DUMMY = new Object();

    private final Map<String, CountDownLatch> pendingReads;

    private final MeterRegistry metricRegistry;
    private final Counter cacheHitsCounter;
    private final Counter cacheMissesCounter;
    private final Counter cacheDeletedMissesCounter; //TODO Debug

    /**
     * Creates a new caching facade with a default meter registry.
     *
     * @param wrapped the underlying facade
     * @param cacheSize the maximum number of cached entries
     * @param cacheOnWrite whether to cache batches on write (write-through)
     */
    public CachingS3BatchFacade(S3BatchFacade wrapped, int cacheSize, boolean cacheOnWrite) {
        this(wrapped, cacheSize, cacheOnWrite, new SimpleMeterRegistry(), "");
    }

    /**
     * Creates a new caching facade.
     *
     * @param wrapped the underlying facade
     * @param cacheSize the maximum number of cached entries
     * @param cacheOnWrite whether to cache batches on write (write-through)
     * @param metricRegistry the meter registry for metrics
     * @param metricsTypeValue a tag value to distinguish cache metrics by type
     */
    public CachingS3BatchFacade(S3BatchFacade wrapped, int cacheSize, boolean cacheOnWrite, MeterRegistry metricRegistry, String metricsTypeValue) {
        this.wrapped = wrapped;
        this.cacheOnWrite = cacheOnWrite;
        this.s3Cache = new S3Cache(cacheSize);
        this.pendingReads =  new ConcurrentHashMap<>();
        this.metricRegistry = metricRegistry;
        this.cacheHitsCounter = Counter
                .builder("s3.cache.hits")
                .description("number of hits on the cache")
                .tag("type", metricsTypeValue)
                .register(metricRegistry);
        this.cacheMissesCounter = Counter
                .builder("s3.cache.misses")
                .description("number of misses on the cache")
                .tag("type", metricsTypeValue)
                .register(metricRegistry);
        this.cacheDeletedMissesCounter = Counter
                .builder("s3.cache.misses.deleted")
                .description("number of deleted misses on the cache")
                .tag("type", metricsTypeValue)
                .register(metricRegistry);
    }

    public void writeBatch(String objectPath, InputStream batchInputStream, int batchSizeInBytes) {
        InputStream maybeWrappedInputStream;
        if (this.cacheOnWrite) {
            byte[] readBatch = loadBatchInputStream(batchInputStream, batchSizeInBytes);
            s3Cache.putObject(objectPath, readBatch); // Maybe put to cache _after_ put to S3 (?)
            this.everCached.put(objectPath, DUMMY); //TODO Temp
            //System.out.println("Write to cache: " + objectPath + " (" + batchSizeInBytes + " bytes)");
            maybeWrappedInputStream = new ByteArrayInputStream(readBatch);
        } else {
            maybeWrappedInputStream = batchInputStream;
        }
        //System.out.println("Write to S3: " + objectPath + " (" + batchSizeInBytes + " bytes)");
        this.wrapped.writeBatch(objectPath, maybeWrappedInputStream, batchSizeInBytes);
    }

    public SizeAwareInputStream readBatch(String objectPath) {
        while (true) {
            byte[] cachedObject = this.s3Cache.getObject(objectPath);
            if (cachedObject != null) {
                //System.out.println("Read from cache: " + objectPath + " (" + cachedObject.length + " bytes)");
                this.cacheHitsCounter.increment();
                return new SizeAwareInputStream(new ByteArrayInputStream(cachedObject), cachedObject.length);
            }

            // Check if someone already requested a read
            CountDownLatch latch = this.pendingReads.putIfAbsent(objectPath, new CountDownLatch(1));
            if (latch == null) {
                // First thread to process this objectPath
                latch = this.pendingReads.get(objectPath);
                try {
                    //System.out.println("Query from S3: " + objectPath + "");
                    if (this.everCached.putIfAbsent(objectPath, DUMMY) != null) { // TODO Temp
                        // Object was already cached before, but is now put to the cache again
                        this.cacheDeletedMissesCounter.increment();
                        //return new SizeAwareInputStream(new ByteArrayInputStream(DUMMY_), readBatch.length);
                    }

                    this.cacheMissesCounter.increment();
                    S3BatchFacade.SizeAwareInputStream batchInputStream = this.wrapped.readBatch(objectPath);
                    byte[] readBatch = loadBatchInputStream(batchInputStream, batchInputStream.size());
                    this.s3Cache.putObject(objectPath, readBatch);
                    return new SizeAwareInputStream(new ByteArrayInputStream(readBatch), readBatch.length);
                } finally {
                    latch.countDown();
                    this.pendingReads.remove(objectPath);
                }
            } else {
                //System.out.println("Wait for previous query to S3: " + objectPath + "");
                // Other threads wait until the first one finishes reading
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    // TODO What to do here?
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @Override
    public SizeAwareInputStream readPartialBatch(String objectPath, int offsetStart, int offsetEnd) {
        throw new UnsupportedOperationException("Partial reads not supported.");
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

    @Override
    public void close() {
        this.wrapped.close();
    }
}
