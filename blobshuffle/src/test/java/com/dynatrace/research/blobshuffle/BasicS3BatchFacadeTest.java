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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
class BasicS3BatchFacadeTest {

    @Container
    private FlociContainer floci = new FlociContainer();

    @Test
    public void testReadAfterWriteWithLargeBatch() throws IOException {
        S3Client s3Client = S3Client
                .builder()
                .endpointOverride(URI.create(floci.getEndpoint()))
                .credentialsProvider(
                        StaticCredentialsProvider.create(
                                AwsBasicCredentials.create(floci.getAccessKey(), floci.getSecretKey())
                        )
                )
                .region(Region.of(floci.getRegion()))
                .build();
        String bucketName = "test-bucket";
        s3Client.createBucket(request -> request.bucket(bucketName));
        ListBucketsResponse listBucketsResponse = s3Client.listBuckets(); // Will show max. 1000
        List<Bucket> allBuckets = listBucketsResponse.buckets();
        Assertions.assertTrue(allBuckets.stream().map(Bucket::name).anyMatch(name -> name.equals(bucketName)));

        BasicS3BatchFacade basicS3BatchFacade = new BasicS3BatchFacade(s3Client, bucketName);

        String objectPath = "the/object/path";
        byte[] batch = new byte[10 * 1024 * 1024];
        Arrays.fill(batch, (byte) 1);

        basicS3BatchFacade.writeBatch(objectPath, new ByteArrayInputStream(batch), batch.length);

        BasicS3BatchFacade.SizeAwareInputStream returnedStream = basicS3BatchFacade.readBatch(objectPath);
        byte[] returnedBatch = new byte[returnedStream.size()];
        int readBytes = returnedStream.readNBytes(returnedBatch, 0, returnedBatch.length);

        assertEquals(batch.length, returnedStream.size());
        assertEquals(batch.length, readBytes);
        assertArrayEquals(batch, returnedBatch);
        assertEquals(-1, returnedStream.read(), "End of stream should have been reached.");
    }

}