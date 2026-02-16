package com.dynatrace.research.s3shuffler;

import com.dynatrace.research.s3shuffler.util.DelegatingInputStream;

import java.io.InputStream;

public interface S3BatchFacade extends AutoCloseable {

    void writeBatch(String objectPath, InputStream batchInputStream, int batchSizeInBytes);

    SizeAwareInputStream readBatch(String objectPath);

    SizeAwareInputStream readPartialBatch(String objectPath, int offsetStart, int offsetEnd);

    @Override
    void close();

    class SizeAwareInputStream extends DelegatingInputStream {

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
