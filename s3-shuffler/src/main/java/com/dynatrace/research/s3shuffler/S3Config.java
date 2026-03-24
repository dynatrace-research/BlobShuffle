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
