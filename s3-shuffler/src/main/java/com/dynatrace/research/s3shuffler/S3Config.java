package com.dynatrace.research.s3shuffler;

import io.micrometer.core.instrument.MeterRegistry;
import software.amazon.awssdk.services.s3.S3Client;

public record S3Config(
        S3Client s3Client,
        String bucketName) {

    public S3BatchFacade toS3BatchFacade(MeterRegistry meterRegistry) {
        return new FullBatchS3BatchFacade(new BasicS3BatchFacade(s3Client, bucketName, meterRegistry));
    }

}
