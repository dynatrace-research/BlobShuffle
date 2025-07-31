package com.dynatrace.research.s3shuffler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

public class CachingS3BatchFacade implements S3BatchFacade {

    private static final Logger LOGGER = LoggerFactory.getLogger(CachingS3BatchFacade.class);

    private final BasicS3BatchFacade wrapped;

    private final boolean cacheOnWrite;

    private final S3Cache s3Cache;

    private final Map<String, CountDownLatch> pendingReads;

    public CachingS3BatchFacade(BasicS3BatchFacade wrapped, int cacheSize, boolean cacheOnWrite) {
        this.wrapped = wrapped;
        this.cacheOnWrite = cacheOnWrite;
        this.s3Cache = new S3Cache(cacheSize);
        this.pendingReads =  new ConcurrentHashMap<>();
    }

    public void writeBatch(String objectPath, InputStream batchInputStream, int batchSizeInBytes) {
        InputStream maybeWrappedInputStream;
        if (this.cacheOnWrite) {
            byte[] readBatch = loadBatchInputStream(batchInputStream, batchSizeInBytes);
            s3Cache.putObject(objectPath, readBatch); // Maybe put to cache _after_ put to S3 (?)
            System.out.println("Write to cache: " + objectPath + " (" + batchSizeInBytes + " bytes)");
            maybeWrappedInputStream = new ByteArrayInputStream(readBatch);
        } else {
            maybeWrappedInputStream = batchInputStream;
        }
        System.out.println("Write to S3: " + objectPath + " (" + batchSizeInBytes + " bytes)");
        this.wrapped.writeBatch(objectPath, maybeWrappedInputStream, batchSizeInBytes);
    }

    public InputStream readBatch(String objectPath) {
        while (true) {
            byte[] cachedObject = this.s3Cache.getObject(objectPath);
            if (cachedObject != null) {
                System.out.println("Read from cache: " + objectPath + " (" + cachedObject.length + " bytes)");
                return new ByteArrayInputStream(cachedObject);
            }

            // Check if someone already requested a read
            CountDownLatch latch = this.pendingReads.putIfAbsent(objectPath, new CountDownLatch(1));
            if (latch == null) {
                // First thread to process this objectPath
                latch = this.pendingReads.get(objectPath);
                try {
                    System.out.println("Query from S3: " + objectPath + "");
                    BasicS3BatchFacade.SizeAwareInputStream batchInputStream = this.wrapped.readBatch(objectPath);
                    byte[] readBatch = loadBatchInputStream(batchInputStream, batchInputStream.size());
                    this.s3Cache.putObject(objectPath, readBatch);
                    return new ByteArrayInputStream(readBatch);
                } finally {
                    latch.countDown();
                    this.pendingReads.remove(objectPath);
                }
            } else {
                System.out.println("Wait for previous query to S3: " + objectPath + "");
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
