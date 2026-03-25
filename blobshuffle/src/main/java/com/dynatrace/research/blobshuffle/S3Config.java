package com.dynatrace.research.blobshuffle;

import io.micrometer.core.instrument.MeterRegistry;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Configuration holding the {@link S3Client} and target bucket name.
 *
 * @param s3Client the AWS {@link S3Client}
 * @param bucketName the name of the S3 bucket used for storing batches
 */
public record S3Config(
        S3Client s3Client,
        String bucketName) {

    /**
     * Creates a {@link S3BatchFacade} from this configuration.
     *
     * @param meterRegistry the meter registry for metrics
     * @return a new {@link S3BatchFacade} instance
     */
    public S3BatchFacade toS3BatchFacade(MeterRegistry meterRegistry) {
        return new FullBatchS3BatchFacade(new BasicS3BatchFacade(s3Client, bucketName, meterRegistry));
    }

}
