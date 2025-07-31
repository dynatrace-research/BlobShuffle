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
