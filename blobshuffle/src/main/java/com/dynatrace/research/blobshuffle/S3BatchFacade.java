package com.dynatrace.research.blobshuffle;

import com.dynatrace.research.blobshuffle.util.DelegatingInputStream;

import java.io.InputStream;

/**
 * Core interface for reading and writing batches to and from object storage.
 */
public interface S3BatchFacade extends AutoCloseable {

    /**
     * Writes a batch to object storage.
     *
     * @param objectPath the object key in the storage bucket
     * @param batchInputStream the input stream containing the batch data
     * @param batchSizeInBytes the size of the batch in bytes
     */
    void writeBatch(String objectPath, InputStream batchInputStream, int batchSizeInBytes);

    /**
     * Reads a complete batch from object storage.
     *
     * @param objectPath the object key in the storage bucket
     * @return an input stream wrapping the batch data with a known size
     */
    SizeAwareInputStream readBatch(String objectPath);

    /**
     * Reads a partial batch (sub-batch) from object storage.
     *
     * @param objectPath the object key in the storage bucket
     * @param offsetStart the start offset (inclusive) of the byte range to read
     * @param offsetEnd the end offset (exclusive) of the byte range to read
     * @return an input stream wrapping the sub-batch data with a known size
     */
    SizeAwareInputStream readPartialBatch(String objectPath, int offsetStart, int offsetEnd);

    @Override
    void close();

    /**
     * An {@link InputStream} that additionally exposes the total size of the stream in bytes.
     */
    class SizeAwareInputStream extends DelegatingInputStream {

        private final int size;

        /**
         * Creates a new size-aware input stream.
         *
         * @param delegate the underlying input stream
         * @param size the total size in bytes
         */
        public SizeAwareInputStream(InputStream delegate, int size) {
            super(delegate);
            this.size = size;
        }

        /**
         * Returns the total size of the stream in bytes.
         *
         * @return the size in bytes
         */
        public int size() {
            return this.size;
        }
    }

}
