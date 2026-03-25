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

import com.dynatrace.hash4j.hashing.Hasher64;
import com.dynatrace.hash4j.hashing.Hashing;
import io.javalin.Javalin;
import io.javalin.http.Header;
import io.javalin.http.HttpStatus;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.common.utils.Utils;
import org.jgroups.JChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;

/**
 * {@link S3BatchFacade} decorator that implements partial batch reads by fetching the full
 * object and slicing the relevant byte range locally.
 */
public class FullBatchS3BatchFacade implements S3BatchFacade {

    private final S3BatchFacade wrapped;

    /**
     * Creates a new decorator wrapping the given facade.
     *
     * @param wrapped the underlying facade
     */
    public FullBatchS3BatchFacade(S3BatchFacade wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public void writeBatch(String objectPath, InputStream batchInputStream, int batchSizeInBytes) {
        this.wrapped.writeBatch(objectPath, batchInputStream, batchSizeInBytes);
    }

    @Override
    public SizeAwareInputStream readBatch(String objectPath) {
        return this.wrapped.readBatch(objectPath);
    }

    @Override
    public SizeAwareInputStream readPartialBatch(String objectPath, int offsetStart, int offsetEnd) {
        return new PartialSizeAwareInputStream(this.wrapped.readBatch(objectPath), offsetStart, offsetEnd);
    }

    @Override
    public void close() {
        this.wrapped.close();
    }

    /**
     * A {@link SizeAwareInputStream} that exposes a contiguous byte range within a larger stream.
     */
    public static class PartialSizeAwareInputStream extends SizeAwareInputStream {

        private int readBytes;

        /**
         * Creates a new partial stream scoped to the given byte range.
         *
         * @param delegate the full-batch stream
         * @param offsetStart the start offset (inclusive)
         * @param offsetEnd the end offset (exclusive)
         */
        public PartialSizeAwareInputStream(SizeAwareInputStream delegate, int offsetStart, int offsetEnd) {
            super(delegate, offsetEnd - offsetStart);
            this.readBytes = -offsetStart;
            // We do not skip here to throw exception later in read methods if offsetStart is beyond the size
        }

        private void forceSkipToStart() throws IOException {
            if (this.readBytes < 0) {
                super.skip(-this.readBytes);
                this.readBytes = 0;
            }
        }

        @Override
        public int read() throws IOException {
            this.forceSkipToStart();
            if (this.readBytes >= size()) {
                return -1;
            }
            int byteRead = super.read();
            if (byteRead != -1) {
                this.readBytes++;
            }
            return byteRead;
        }

        @Override
        public int read(byte[] b) throws IOException {
            return read(b, 0, b.length);
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            this.forceSkipToStart();
            if (this.readBytes >= size()) {
                return -1;
            }
            int bytesToRead = Math.min(len, size() - this.readBytes);
            int actuallyRead = super.read(b, off, bytesToRead);
            if (actuallyRead != -1) {
                this.readBytes += actuallyRead;
            }
            return actuallyRead;
        }

        @Override
        public byte[] readAllBytes() throws IOException {
            return this.readNBytes(size());
        }

        @Override
        public byte[] readNBytes(int len) throws IOException {
            this.forceSkipToStart();
            int bytesToRead = Math.min(len, size() - this.readBytes);
            byte[] bytesRead = super.readNBytes(bytesToRead);
            this.readBytes += bytesRead.length;
            return bytesRead;
        }

        @Override
        public int readNBytes(byte[] b, int off, int len) throws IOException {
            this.forceSkipToStart();
            if (this.readBytes >= size()) {
                return -1;
            }
            int bytesToRead = Math.min(len, size() - this.readBytes);
            int actuallyRead = super.readNBytes(b, off, bytesToRead);
            if (actuallyRead != -1) {
                this.readBytes += actuallyRead;
            }
            return actuallyRead;
        }

        @Override
        public long skip(long n) throws IOException {
            this.forceSkipToStart();
            long bytesToSkip = Math.min(n, size() - this.readBytes);
            long actuallySkipped = super.skip(bytesToSkip);
            this.readBytes += actuallySkipped;
            return actuallySkipped;
        }

        @Override
        public void skipNBytes(long n) throws IOException {
            this.forceSkipToStart();
            long bytesToSkip = Math.min(n, size() - this.readBytes);
            super.skipNBytes(bytesToSkip);
            this.readBytes += bytesToSkip;
        }

        @Override
        public int available() throws IOException {
            this.forceSkipToStart();
            int availableInDelegate = super.available();
            int remainingBytes = size() - this.readBytes;
            return Math.min(availableInDelegate, remainingBytes);
        }

        @Override
        public void close() throws IOException {
            super.close();
        }

        @Override
        public synchronized void mark(int readlimit) {
            //super.mark(readlimit);  // Could add later
            // Do nothing as per InputStream contract
        }

        @Override
        public synchronized void reset() throws IOException {
            //super.reset(); // Could add later, but we need to be careful with the readBytes counter
            throw new IOException("mark/reset not supported");
        }

        @Override
        public boolean markSupported() {
            //return super.markSupported(); // Could add later
            return false;
        }

        @Override
        public long transferTo(OutputStream out) throws IOException {
            // Could implement later if required
            throw new UnsupportedOperationException("transferTo not supported");
        }

    }

}
