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

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.processor.api.Record;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Reads serialized Kafka records from a batch byte stream, performing the inverse
 * of {@link BatchCollector}.
 *
 * <p>For each record, the read methods must be called in the following order:
 * <ol>
 *   <li>{@link #readRecordPartition()}</li>
 *   <li>{@link #readRecordKey()}</li>
 *   <li>{@link #readRecordValue()}</li>
 *   <li>{@link #readRecordTimestamp()}</li>
 *   <li>{@link #readHeaders()}</li>
 * </ol>
 * Use {@link #hasNextRecord()} to check whether more records are available before reading.
 */
public class BatchReader implements AutoCloseable {

    private final InputStream batchInputStream;

    private int remainingBytes;


    /**
     * Creates a new batch reader.
     *
     * @param inputStream the input stream containing the batch data
     * @param offsetBytes the number of bytes to skip from the start of the stream
     * @param length the number of bytes to read
     */
    public BatchReader(InputStream inputStream, int offsetBytes, int length) {
        this.batchInputStream = inputStream;
        this.remainingBytes = length;

        this.skip(offsetBytes);
    }

    /**
     * Returns whether there are more records to read.
     *
     * @return {@code true} if more records are available
     */
    public boolean hasNextRecord() {
        return remainingBytes > 0;
    }

    /**
     * Reads and returns the destination partition number of the next record.
     *
     * @return the partition number
     */
    public int readRecordPartition() {
        return this.readInt();
    }

    /**
     * Reads and returns the key bytes of the next record.
     *
     * @return the key bytes, or {@code null} if the key is null
     */
    public byte[] readRecordKey() {
        int recordKeyLength = this.readInt();
        if (recordKeyLength == -1) {
            return null;
        } else {
            return this.readExactNBytes(recordKeyLength);
        }
    }

    /**
     * Reads and returns the value bytes of the next record.
     *
     * @return the value bytes, or {@code null} if the value is null
     */
    public byte[] readRecordValue() {
        int recordValueLength = this.readInt();
        if (recordValueLength == -1) {
            return null;
        } else {
            return this.readExactNBytes(recordValueLength);
        }
    }

    /**
     * Reads and returns the timestamp of the next record.
     *
     * @return the timestamp in milliseconds
     */
    public long readRecordTimestamp() {
        return this.readLong();
    }

    /**
     * Reads and returns the headers of the next record.
     *
     * @return the list of headers
     */
    public List<Header> readHeaders() {
        int recordHeadersLength = this.readInt();
        List<Header> recordHeaders = new ArrayList<>(recordHeadersLength);
        for (int recordHeaderIdx = 0; recordHeaderIdx < recordHeadersLength; recordHeaderIdx++) {
            // Get record header key
            int recordHeaderKeyLength = this.readInt();
            String recordHeaderKey = new String(this.readExactNBytes(recordHeaderKeyLength), StandardCharsets.UTF_8);
            // Get record value
            int recordHeaderValueLength = this.readInt();
            byte[] recordHeaderValue = this.readExactNBytes(recordHeaderValueLength);
            // Add header to recordHeaders
            recordHeaders.add(new RecordHeader(recordHeaderKey, recordHeaderValue));
        }
        return recordHeaders;
    }

    private long skip(long n) {
        try {
            return this.batchInputStream.skip(n);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private byte[] readNBytesSuppressThrows(int len) {
        try {
            return this.batchInputStream.readNBytes(len);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private byte[] readExactNBytes(int len) {
        byte[] longBytes = this.readNBytesSuppressThrows(len);
        // check if we reached the end of the input stream
        if (longBytes.length < len) {
            throw new RuntimeException("Could not read " + len + " bytes as not enough bytes available");
        }
        remainingBytes -= longBytes.length;
        return longBytes;
    }

    private int readInt() {
        return ByteBuffer.wrap(readExactNBytes(Integer.BYTES)).getInt();
    }

    private long readLong() {
        return ByteBuffer.wrap(readExactNBytes(Long.BYTES)).getLong();
    }

    @Override
    public void close() {
        try {
            this.batchInputStream.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
