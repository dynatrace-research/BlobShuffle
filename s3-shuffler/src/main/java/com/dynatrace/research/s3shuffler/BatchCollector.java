package com.dynatrace.research.s3shuffler;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.io.InputStream;
import java.nio.Buffer;
import java.nio.BufferOverflowException;
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

        buffer.mark(); // Mark the current position to reset in case of overflow

        try {
            this.addRecordToBuffer(buffer, outputPartition, recordKey, recordValue, recordTimestamp, recordHeaders);
        } catch (BufferOverflowException e) {
            buffer.reset(); // Reset buffer to the marked position to discard the partially written record
            throw new IllegalStateException("Cannot add record to batch for partition " + outputPartition + " as it would exceed the maximum batch size of " + batchMaxBytes + " bytes", e);
        }

        // Update metadata
        this.highestTimestamp = Math.max(this.highestTimestamp, recordTimestamp);
        this.numRecords++;
    }

    private void addRecordToBuffer(ByteBuffer buffer, int outputPartition, byte[] recordKey, byte[] recordValue, long recordTimestamp, Headers recordHeaders) throws BufferOverflowException {
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
