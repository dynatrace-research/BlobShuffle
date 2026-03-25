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

import com.dynatrace.research.blobshuffle.util.DelegatingInputStream;

import java.io.InputStream;

/**
 * Core interface for reading and writing batches to and from object storage.
 */
public interface S3BatchFacade extends AutoCloseable {

    /**
     * Writes a batch to the object storage service.
     *
     * @param objectPath the object key in the storage bucket
     * @param batchInputStream the input stream containing the batch data
     * @param batchSizeInBytes the size of the batch in bytes
     */
    void writeBatch(String objectPath, InputStream batchInputStream, int batchSizeInBytes);

    /**
     * Reads a complete batch from the object storage service.
     *
     * @param objectPath the object key in the storage bucket
     * @return an input stream wrapping the batch data with a known size
     */
    SizeAwareInputStream readBatch(String objectPath);

    /**
     * Reads a partial batch (sub-batch) from the object storage service.
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
