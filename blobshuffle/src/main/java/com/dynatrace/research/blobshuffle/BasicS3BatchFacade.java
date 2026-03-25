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
        PutObjectResponse response = this.putsTimer.record(() ->
                s3Client.putObject(request ->
                                request
                                        .bucket(bucketName)
                                        .key(objectPath),
                        RequestBody.fromInputStream(batchInputStream, batchSizeInBytes))
        );
        this.putsCounter.increment();
        try {
            batchInputStream.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        // String eTag = response.eTag(); // Could be used as integrity check
        LOGGER.info("Wrote batch of {} bytes to object {}.", batchSizeInBytes, objectPath);
    }

    @Override
    public SizeAwareInputStream readBatch(String objectPath) {
        LOGGER.info("Fetch batch from object {}.", objectPath);
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

}
