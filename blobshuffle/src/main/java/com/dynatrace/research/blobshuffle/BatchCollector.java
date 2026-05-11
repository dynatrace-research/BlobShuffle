package com.dynatrace.research.blobshuffle;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.Buffer;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Collects serialized records into per-partition byte buffers for batch assembly.
 * Records for all partitions in the same target AZ are concatenated into a single batch.
 */
public class BatchCollector {

    private static final Logger LOGGER = LoggerFactory.getLogger(BatchCollector.class);

    private final int batchMaxBytes;

    private final NavigableMap<Integer, ByteBuffer> byteBuffers = new TreeMap<>(); // TODO optimize maybe

    private int numRecords;

    private long highestTimestamp = -1;

    /**
     * Creates a new batch collector.
     *
     * @param batchMaxBytesPerPartition the maximum buffer size per partition in bytes
     */
    public BatchCollector(int batchMaxBytesPerPartition) {
        this.batchMaxBytes = batchMaxBytesPerPartition;
    }

    /**
     * Adds a record to the buffer for the given output partition.
     *
     * @param outputPartition the target partition number
     * @param recordKey the serialized record key, may be {@code null}
     * @param recordValue the serialized record value, may be {@code null}
     * @param recordTimestamp the record timestamp
     * @param recordHeaders the record headers
     * @throws IllegalStateException if the record would exceed the maximum buffer size for the partition
     */
    public void addRecord(int outputPartition, byte[] recordKey, byte[] recordValue, long recordTimestamp, Headers recordHeaders) {
        ByteBuffer buffer = byteBuffers.computeIfAbsent(outputPartition, d -> ByteBuffer.allocate(batchMaxBytes));

        buffer.mark(); // Mark the current position to reset in case of overflow

        try {
            this.addRecordToBuffer(buffer, outputPartition, recordKey, recordValue, recordTimestamp, recordHeaders);
        } catch (BufferOverflowException e) {
            buffer.reset(); // Reset buffer to the marked position to discard the partially written record
            // Grow the buffer and retry
            buffer = growBuffer(buffer);
            byteBuffers.put(outputPartition, buffer);
            LOGGER.info("Resized buffer for partition {} from {} to {} bytes.", outputPartition, buffer.capacity() / 2, buffer.capacity());
            this.addRecordToBuffer(buffer, outputPartition, recordKey, recordValue, recordTimestamp, recordHeaders);
        }

        // Update metadata
        this.highestTimestamp = Math.max(this.highestTimestamp, recordTimestamp);
        this.numRecords++;
    }

    private static ByteBuffer growBuffer(ByteBuffer original) {
        int newCapacity = original.capacity() * 2;
        ByteBuffer grown = ByteBuffer.allocate(newCapacity);
        original.flip();
        grown.put(original);
        return grown;
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

    /**
     * Returns the total size in bytes across all partition buffers.
     *
     * @return the total size in bytes
     */
    public int getSizeInBytes() {
        // Could also be implemented as a running sum, incremented on every added record
        return this.byteBuffers.values().stream().mapToInt(Buffer::position).sum();
    }

    /**
     * Returns offset ranges for all partitions that have buffered data.
     *
     * @return a sorted map from partition number to byte range within the serialized batch
     */
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

    /**
     * Returns the byte range occupied by the given partition within the serialized batch.
     *
     * @param partition the partition number
     * @return the offset range
     */
    public OffsetRange getOffsetRangeForPartition(int partition) {
        int offsetStart = this.byteBuffers.headMap(partition).values().stream().mapToInt(Buffer::position).sum();
        int offsetEnd = offsetStart + this.byteBuffers.get(partition).position();
        return new OffsetRange(offsetStart, offsetEnd);
    }

    /**
     * Returns the highest timestamp among all collected records.
     *
     * @return the highest timestamp, or {@code -1} if no records have been added
     */
    public long getHighestTimestamp() {
        return this.highestTimestamp;
    }

    /**
     * Returns the number of collected records.
     *
     * @return the record count
     */
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
        return new BatchCollectorInputStream(this.byteBuffers.values());
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

    /**
     * Represents a contiguous byte range within a batch.
     *
     * @param startInclusive the start offset (inclusive)
     * @param endExclusive the end offset (exclusive)
     */
    public record OffsetRange(int startInclusive, int endExclusive) {}

    static class BatchCollectorInputStream extends RewindableInputStream {

        private final Collection<ByteBuffer> buffers;

        private Iterator<ByteBuffer> iterator;

        private ByteBuffer currentBuffer;

        BatchCollectorInputStream(Collection<ByteBuffer> buffers) {
            this.buffers = buffers;
            for (ByteBuffer buffer : buffers) {
                buffer.flip();
            }
            this.iterator = buffers.iterator();
            this.currentBuffer = this.iterator.hasNext() ? this.iterator.next() : null;
        }

        @Override
        public void rewind() {
            for (ByteBuffer buffer : this.buffers) {
                buffer.rewind();
            }
            this.iterator = this.buffers.iterator();
            if (this.iterator.hasNext()) {
                this.currentBuffer = this.iterator.next();
            } else {
                this.currentBuffer = null;
            }
        }

        @Override
        public int read() {
            if (this.currentBuffer == null) {
                return -1;
            }
            while (!this.currentBuffer.hasRemaining()) {
                if (this.iterator.hasNext()) {
                    this.currentBuffer = this.iterator.next();
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
                int readLen = Math.min(this.currentBuffer.remaining(), len - effectiveLen);
                this.currentBuffer.get(bytes, off + effectiveLen, readLen);
                effectiveLen += readLen;
                while (this.currentBuffer != null && !this.currentBuffer.hasRemaining() && this.iterator.hasNext()) {
                    this.currentBuffer = this.iterator.next();
                }
            }
            if (effectiveLen == 0 && len > 0) {
                return -1; // Requested bytes, but no bytes available: return -1 according to InputStream contract
            }
            return effectiveLen;
        }


    }

}
