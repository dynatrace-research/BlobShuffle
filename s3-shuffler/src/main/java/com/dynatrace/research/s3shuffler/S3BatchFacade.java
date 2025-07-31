package com.dynatrace.research.s3shuffler;

import java.io.InputStream;

public interface S3BatchFacade {

    void writeBatch(String objectPath, InputStream batchInputStream, int batchSizeInBytes);

    InputStream readBatch(String objectPath);

}
