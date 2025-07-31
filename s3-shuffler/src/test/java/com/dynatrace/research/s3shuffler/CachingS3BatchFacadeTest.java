package com.dynatrace.research.s3shuffler;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

@Testcontainers
class CachingS3BatchFacadeTest {

    @Container
    private LocalStackContainer localstack = new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.5.0"))
            .withServices(S3);

    @Test
    public void testReadAfterWriteWithLargeBatchAndNoCaching() throws IOException {
        S3Client s3Client = S3Client
                .builder()
                .endpointOverride(localstack.getEndpoint())
                .credentialsProvider(
                        StaticCredentialsProvider.create(
                                AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey())
                        )
                )
                .region(Region.of(localstack.getRegion()))
                .build();
        String bucketName = "test-bucket";
        s3Client.createBucket(request -> request.bucket(bucketName));
        ListBucketsResponse listBucketsResponse = s3Client.listBuckets(); // Will show max. 1000
        List<Bucket> allBuckets = listBucketsResponse.buckets();
        Assertions.assertTrue(allBuckets.stream().map(Bucket::name).anyMatch(name -> name.equals(bucketName)));

        CachingS3BatchFacade basicS3BatchFacade = new CachingS3BatchFacade(
                new BasicS3BatchFacade(s3Client, bucketName),
                0,
                false);

        String objectPath = "the/object/path";
        byte[] batch = new byte[10 * 1024 * 1024];
        Arrays.fill(batch, (byte) 1);

        basicS3BatchFacade.writeBatch(objectPath, new ByteArrayInputStream(batch), batch.length);

        try (InputStream returnedStream = basicS3BatchFacade.readBatch(objectPath)) {
            byte[] returnedBatch = new byte[batch.length];
            int readBytes = returnedStream.readNBytes(returnedBatch, 0, returnedBatch.length);

            assertEquals(batch.length, readBytes);
            assertArrayEquals(batch, returnedBatch);
            assertEquals(-1, returnedStream.read(), "End of stream should have been reached.");
        }
    }

}