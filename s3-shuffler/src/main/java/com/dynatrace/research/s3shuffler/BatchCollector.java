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

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.io.InputStream;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class BatchCollector {

    private final int batchMaxBytes;

    private final NavigableMap<Integer, ByteBuffer> byteBuffers = new TreeMap<>(); // TODO optimize maybe

    private int numRecords;

    private long highestTimestamp = -1;

    public BatchCollector(int batchMaxBytesPerPartition) {
        this.batchMaxBytes = batchMaxBytesPerPartition;
    }

    public void addRecord(int outputPartition, byte[] recordKey, byte[] recordValue, long recordTimestamp, Headers recordHeaders) {
        ByteBuffer buffer = byteBuffers.computeIfAbsent(outputPartition, d -> ByteBuffer.allocate(batchMaxBytes));

        // Add partition
        buffer.putInt(outputPartition); // partition

        // Add record key
        if (recordKey == null) {
            buffer.putInt(-1); // no key
        }
        else {
            buffer.putInt(recordKey.length); // key size
            buffer.put(recordKey); // key content
        }

        // Add record value
        if (recordValue == null) {
            buffer.putInt(-1); // no value
        }
        else {
            buffer.putInt(recordValue.length); // value size
            buffer.put(recordValue); // value content
        }

        // Add timestamp
        buffer.putLong(recordTimestamp); // timestamp

        // Add headers
        Header[] recordHeadersArray = recordHeaders.toArray();
        buffer.putInt(recordHeadersArray.length); // number of headers
        for (Header header : recordHeadersArray) {
            byte[] headerKey = header.key().getBytes(StandardCharsets.UTF_8);
            byte[] headerValue = header.value();
            buffer.putInt(headerKey.length); // header key size
            buffer.put(headerKey);  // header key content
            buffer.putInt(headerValue.length); // header value size
            buffer.put(headerValue); // header value content
        }

        // Update metadata
        this.highestTimestamp = Math.max(this.highestTimestamp, recordTimestamp);
        this.numRecords++;
    }

    public int getSizeInBytes() {
        // Could also be implemented as a running sum, incremented on every added record
        return this.byteBuffers.values().stream().mapToInt(Buffer::position).sum();
    }

    public SortedMap<Integer,OffsetRange> getOffsetRangeForAllPartition() {
        int offsetStart = 0;
        SortedMap<Integer,OffsetRange> offsetRanges = new TreeMap<>();
        for (Map.Entry<Integer, ByteBuffer> byteBufferEntry : this.byteBuffers.entrySet()) {
            int bufferSize = byteBufferEntry.getValue().position();
            if (bufferSize > 0) {
                int offsetEnd = offsetStart + bufferSize;
                offsetRanges.put(byteBufferEntry.getKey(), new OffsetRange(offsetStart, offsetEnd));
                offsetStart = offsetEnd;
            }
        }
        return offsetRanges;
    }

    public OffsetRange getOffsetRangeForPartition(int partition) {
        int offsetStart = this.byteBuffers.headMap(partition).values().stream().mapToInt(Buffer::position).sum();
        int offsetEnd = offsetStart + this.byteBuffers.get(partition).position();
        return new OffsetRange(offsetStart, offsetEnd);
    }

    public long getHighestTimestamp() {
        return this.highestTimestamp;
    }

    public int getNumRecords() {
        return this.numRecords;
    }

    /**
     * Returns an {@link InputStream} for retrieving the batched data. After calling this method it is forbidden to make
     * any calls to any other method as long as the {@link InputStream} is read and {@link BatchCollector#reset()} has
     * not been called.
     * <p>
     * The returned {@link InputStream} only provides efficient implementations for {@link InputStream#read()} and
     * {@link InputStream#read(byte[], int, int)}.
     *
     * @return the input stream
     */
    public InputStream getAsInputStream() {
        return new BatchCollectorInputStream(this.byteBuffers.values().iterator());
    }

    /**
     * Resets this {@link BatchCollector} in a way that all batched data is cleared, but the allocated underlying memory
     * is kept. That means that the allocated memory of a {@link BatchCollector} can grow over time if it is repeatedly
     * used to store data for different partitions.
     */
    public void reset() {
        for (ByteBuffer byteBuffer : this.byteBuffers.values()) {
            byteBuffer.clear();
        }
        this.highestTimestamp = -1;
        this.numRecords = 0;
    }

    public record OffsetRange(int startInclusive, int endExclusive) {}

    private static class BatchCollectorInputStream extends InputStream {

        private final Iterator<ByteBuffer> iterator;

        private ByteBuffer currentBuffer;

        public BatchCollectorInputStream(Iterator<ByteBuffer> iterator) {
            this.iterator = iterator;
            if (this.iterator.hasNext()) {
                this.currentBuffer = this.iterator.next();
                this.currentBuffer.flip();
            }
        }

        @Override
        public int read() {
            if (this.currentBuffer == null) {
                return -1;
            }
            while (!this.currentBuffer.hasRemaining()) {
                if (iterator.hasNext()) {
                    this.currentBuffer = this.iterator.next();
                    this.currentBuffer.flip();
                } else {
                    return -1;
                }
            }
            return this.currentBuffer.get() & 0xFF;
        }

        @Override
        public int read(byte[] bytes, int off, int len) {
            int effectiveLen = 0;
            while (effectiveLen < len && this.currentBuffer != null && this.currentBuffer.hasRemaining()) {
                int readLen = Math.min(Math.min(len, this.currentBuffer.remaining()), len - effectiveLen);
                this.currentBuffer.get(bytes, effectiveLen, readLen);
                effectiveLen += readLen;
                while (!this.currentBuffer.hasRemaining() && this.iterator.hasNext()) {
                    this.currentBuffer = this.iterator.next();
                    this.currentBuffer.flip();
                }
            }
            if (effectiveLen == 0 && len > 0) {
                return -1; // Requested bytes, but no bytes available: return -1 according to InputStream contract
            }
            return effectiveLen;
        }


    }

}
