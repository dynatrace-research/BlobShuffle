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

import io.floci.testcontainers.FlociContainer;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.apache5.Apache5HttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;

/**
 * Measures raw S3 write and read latency against FlociContainer to establish
 * a baseline.  This test makes no assertions on latency — it only logs timings
 * so that CI output can show exactly how fast (or slow) the S3 layer is.
 * Compare these numbers against the 30 s drain window in the E2E test to
 * understand whether S3 is the bottleneck.
 */
@Testcontainers
class S3LatencyTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3LatencyTest.class);

    private static final int BATCH_SIZE_BYTES = 1024 * 1024; // 1 MB — same as E2E batchDesiredBytes
    private static final int REPETITIONS = 5;

    @Container
    private final FlociContainer floci = new FlociContainer();

    @Test
    void testS3WriteReadLatency() throws IOException {
        S3Client s3Client = S3Client.builder()
                .httpClient(Apache5HttpClient.builder().maxConnections(100).build())
                .endpointOverride(URI.create(floci.getEndpoint()))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(floci.getAccessKey(), floci.getSecretKey())))
                .region(Region.of(floci.getRegion()))
                .build();

        String bucketName = "latency-test-bucket";
        s3Client.createBucket(r -> r.bucket(bucketName));

        BasicS3BatchFacade facade = new BasicS3BatchFacade(s3Client, bucketName);

        byte[] data = new byte[BATCH_SIZE_BYTES];
        Arrays.fill(data, (byte) 42);

        long[] writeTimes = new long[REPETITIONS];
        long[] readTimes = new long[REPETITIONS];

        for (int i = 0; i < REPETITIONS; i++) {
            String path = "batch-" + i;

            long writeStart = System.currentTimeMillis();
            facade.writeBatch(path, new ByteArrayInputStream(data), data.length);
            writeTimes[i] = System.currentTimeMillis() - writeStart;

            long readStart = System.currentTimeMillis();
            BasicS3BatchFacade.SizeAwareInputStream stream = facade.readBatch(path);
            stream.readAllBytes();
            stream.close();
            readTimes[i] = System.currentTimeMillis() - readStart;

            LOGGER.info("Batch {}: write={}ms  read={}ms  round-trip={}ms",
                    i, writeTimes[i], readTimes[i], writeTimes[i] + readTimes[i]);
        }

        long maxWrite = Arrays.stream(writeTimes).max().getAsLong();
        long maxRead  = Arrays.stream(readTimes).max().getAsLong();
        long avgWrite = (long) Arrays.stream(writeTimes).average().getAsDouble();
        long avgRead  = (long) Arrays.stream(readTimes).average().getAsDouble();

        LOGGER.info("S3 write  — avg={}ms  max={}ms", avgWrite, maxWrite);
        LOGGER.info("S3 read   — avg={}ms  max={}ms", avgRead,  maxRead);
        LOGGER.info("Round-trip— avg={}ms  max={}ms", avgWrite + avgRead, maxWrite + maxRead);
        LOGGER.info("E2E drain budget: the last batch needs at most (write + read) ms to clear the pipeline.");
    }
}
