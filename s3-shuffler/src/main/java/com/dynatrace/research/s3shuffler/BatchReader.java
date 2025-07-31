package com.dynatrace.research.s3shuffler;

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

public class BatchReader implements AutoCloseable {

    private final InputStream batchInputStream;

    private int remainingBytes;


    public BatchReader(InputStream inputStream, int offsetBytes, int length) {
        this.batchInputStream = inputStream;
        this.remainingBytes = length;

        this.skip(offsetBytes);
    }

    public boolean hasNextRecord() {
        return remainingBytes > 0;
    }

    public int readRecordPartition() {
        return this.readInt();
    }

    public byte[] readRecordKey() {
        int recordKeyLength = this.readInt();
        if (recordKeyLength == -1) {
            return null;
        } else {
            return this.readExactNBytes(recordKeyLength);
        }
    }

    public byte[] readRecordValue() {
        int recordValueLength = this.readInt();
        if (recordValueLength == -1) {
            return null;
        } else {
            return this.readExactNBytes(recordValueLength);
        }
    }

    public long readRecordTimestamp() {
        return this.readLong();
    }

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
        remainingBytes -= longBytes.length;
        // check if we reached the end of the input stream
        if (longBytes.length < len || remainingBytes < 0) {
            throw new RuntimeException("Could not read " + len + " bytes as not enough bytes available");
        }
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
