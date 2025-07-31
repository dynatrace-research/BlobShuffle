package com.dynatrace.research.s3shuffler;

import com.dynatrace.research.s3shuffler.util.DelegatingInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.io.IOException;
import java.io.InputStream;

public class BasicS3BatchFacade implements S3BatchFacade {

    private static final Logger LOGGER = LoggerFactory.getLogger(BasicS3BatchFacade.class);

    private final S3Client s3Client;

    private final String bucketName;

    public BasicS3BatchFacade(S3Client s3Client, String bucketName) {
        this.s3Client = s3Client;
        this.bucketName = bucketName;
    }

    @Override
    public void writeBatch(String objectPath, InputStream batchInputStream, int batchSizeInBytes) {
        PutObjectResponse response = s3Client.putObject(request ->
                        request
                                .bucket(bucketName)
                                .key(objectPath),
                RequestBody.fromInputStream(batchInputStream, batchSizeInBytes));
        try {
            batchInputStream.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        String eTag = response.eTag(); // Could be used as integrity check
        LOGGER.info("Wrote batch of {} bytes to object {}.", batchSizeInBytes, objectPath);
    }

    @Override
    public SizeAwareInputStream readBatch(String objectPath) {
        LOGGER.info("Fetch batch from object {}.", objectPath);
        ResponseInputStream<GetObjectResponse> batchInputStream = s3Client.getObject(request -> request
                .bucket(this.bucketName)
                .key(objectPath));
        long batchSize = batchInputStream.response().contentLength(); // TODO check if works as expected
        if (batchSize >= Integer.MAX_VALUE) {
            throw new RuntimeException("Object " + objectPath + " too large for a byte[]");
        }
        return new SizeAwareInputStream(batchInputStream, (int) batchSize);
    }

    public S3BatchFacade withCache(int cacheSize, boolean cacheOnWrite) {
        return new CachingS3BatchFacade(this, cacheSize, cacheOnWrite);
    }

    public static class SizeAwareInputStream extends DelegatingInputStream {

        private final int size;

        public SizeAwareInputStream(InputStream delegate, int size) {
            super(delegate);
            this.size = size;
        }

        public int size() {
            return this.size;
        }
    }

}
