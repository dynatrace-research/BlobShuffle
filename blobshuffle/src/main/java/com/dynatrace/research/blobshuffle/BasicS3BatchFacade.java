package com.dynatrace.research.blobshuffle;

import com.dynatrace.research.blobshuffle.util.DelegatingInputStream;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;

/**
 * {@link S3BatchFacade} implementation that performs direct PUT and GET operations on S3 or a compatible object storage
 * service.
 */
public class BasicS3BatchFacade implements S3BatchFacade {

    private static final Logger LOGGER = LoggerFactory.getLogger(BasicS3BatchFacade.class);

    private final S3Client s3Client;

    private final String bucketName;

    private final MeterRegistry meterRegistry;

    private final Counter putsCounter;

    private final Counter getsCounter;

    private final Timer putsTimer;

    private final Timer getsTimer;

    /**
     * Creates a new facade with a default meter registry ({@link SimpleMeterRegistry}).
     *
     * @param s3Client the AWS {@link S3Client}
     * @param bucketName the S3 bucket name
     */
    public BasicS3BatchFacade(S3Client s3Client, String bucketName) {
        this(s3Client, bucketName, new SimpleMeterRegistry());
    }

    /**
     * Creates a new facade.
     *
     * @param s3Client the AWS {@link S3Client}
     * @param bucketName the S3 bucket name
     * @param meterRegistry the meter registry for metrics
     */
    public BasicS3BatchFacade(S3Client s3Client, String bucketName, MeterRegistry meterRegistry) {
        this.s3Client = s3Client;
        this.bucketName = bucketName;
        this.meterRegistry = meterRegistry;
        this.putsCounter = Counter
                .builder("s3.objects.puts")
                .description("number of PUT requests to S3")
                .register(meterRegistry);
        this.getsCounter = Counter
                .builder("s3.objects.gets")
                .description("number of GET requests to S3")
                .register(meterRegistry);
        this.putsTimer = Timer.builder("s3.objects.put.latency")
                .description("time for PUT requests to S3") // optional
                //.tags("region", "test") // optional
                .publishPercentiles(0.5, 0.95, 0.99, 0.999) // median and 95th, 99th and 99.9th percentile
                .publishPercentileHistogram()
                //.serviceLevelObjectives(Duration.ofMillis(100))
                .minimumExpectedValue(Duration.ofMillis(1))
                .maximumExpectedValue(Duration.ofSeconds(60))
                .register(meterRegistry);
        this.getsTimer = Timer.builder("s3.objects.get.latency")
                .description("time for GET requests to S3") // optional
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
        InputStream s3Input = batchInputStream instanceof RewindableInputStream rewindable
                ? new RetryableInputStream(rewindable)
                : batchInputStream;
        PutObjectResponse response = this.putsTimer.record(() ->
                s3Client.putObject(request ->
                                request
                                        .bucket(bucketName)
                                        .key(objectPath),
                        RequestBody.fromInputStream(s3Input, batchSizeInBytes))
        );
        this.putsCounter.increment();
        try {
            batchInputStream.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        // String eTag = response.eTag(); // Could be used as integrity check
        LOGGER.debug("Wrote batch of {} bytes to object {}.", batchSizeInBytes, objectPath);
    }

    @Override
    public SizeAwareInputStream readBatch(String objectPath) {
        LOGGER.debug("Fetch batch from object {}.", objectPath);
        ResponseInputStream<GetObjectResponse> batchInputStream = this.getsTimer.record(() ->
                s3Client.getObject(request ->
                        request
                                .bucket(this.bucketName)
                                .key(objectPath))
        );
        this.getsCounter.increment();
        long batchSize = batchInputStream.response().contentLength(); // TODO check if works as expected
        if (batchSize >= Integer.MAX_VALUE) {
            throw new RuntimeException("Object " + objectPath + " too large for a byte[]");
        }
        return new SizeAwareInputStream(batchInputStream, (int) batchSize);
    }

    @Override
    public SizeAwareInputStream readPartialBatch(String objectPath, int offsetStart, int offsetEnd) {
        // Might change later to use Range GETs from S3
        throw new UnsupportedOperationException("Partial reads from S3 not supported");
    }

    @Override
    public void close() {

    }

    /**
     * Wraps a {@link RewindableInputStream} to support {@link #markSupported()}, {@link #mark(int)},
     * and {@link #reset()} as required by the AWS SDK for retryable PUT requests.
     *
     * <p>The SDK always marks at position 0 before reading the body, then calls {@link #reset()}
     * to rewind for each retry attempt. Therefore, {@link #mark(int)} is a no-op and
     * {@link #reset()} simply delegates to {@link RewindableInputStream#rewind()}.
     */
    private static class RetryableInputStream extends DelegatingInputStream {

        private final RewindableInputStream rewindable;

        RetryableInputStream(RewindableInputStream rewindable) {
            super(rewindable);
            this.rewindable = rewindable;
        }

        @Override
        public boolean markSupported() {
            return true;
        }

        @Override
        public void mark(int readlimit) {}

        @Override
        public void reset() {
            rewindable.rewind();
        }
    }

}
