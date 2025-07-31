package com.dynatrace.research.s3shuffler;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.SortedMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

public class BatchCollectorTest {

    private static final int LARGE_MAX_BYTES_PER_PARTITION = 10 * 1024 * 1024; // 10 MB

    private static final int SMALL_MAX_BYTES_PER_PARTITION = 1024; // 1 KB

    @Test
    public void testPartitionsCreated() {
        BatchCollector batchCollector = createPopulatedBatchCollector();
        assertEquals(
                IntStream.rangeClosed(1, 10).boxed().collect(Collectors.toList()),
                List.copyOf(batchCollector.getOffsetRangeForAllPartition().keySet()));
    }

    @Test
    public void testPartitionsOffsetsHaveNoGaps() {
        BatchCollector batchCollector = createPopulatedBatchCollector();
        int previousEnd = 0;
        for (BatchCollector.OffsetRange range : batchCollector.getOffsetRangeForAllPartition().values()) {
            assertEquals(previousEnd, range.startInclusive());
            previousEnd = range.endExclusive();
        }
        assertEquals(batchCollector.getSizeInBytes(), previousEnd);
    }

    @Test
    public void testInputStreamReadByte() throws IOException {
        BatchCollector batchCollector = createPopulatedBatchCollector();
        int batchCollectorSize = batchCollector.getSizeInBytes();
        SortedMap<Integer, BatchCollector.OffsetRange> batchCollectorOffsetRanges = batchCollector.getOffsetRangeForAllPartition();
        InputStream inputStream = batchCollector.getAsInputStream();
        byte[] output = new byte[batchCollectorSize];
        for (int i = 0; i < batchCollectorSize; i++) {
            int readReturn = inputStream.read();
            Assertions.assertNotEquals(-1, readReturn, "Input stream is shorter than it should be.");
            output[i] = (byte) readReturn;
        }
        int lastReadReturn = inputStream.read();
        Assertions.assertEquals(-1, lastReadReturn, "Input stream is longer than it should be.");

        assertOutputArray(batchCollectorOffsetRanges, output);
    }

    @Test
    public void testInputStreamReadByteArrayWithTooLargeArray() throws IOException {
        BatchCollector batchCollector = createPopulatedBatchCollector();
        int batchCollectorSize = batchCollector.getSizeInBytes();
        SortedMap<Integer, BatchCollector.OffsetRange> batchCollectorOffsetRanges = batchCollector.getOffsetRangeForAllPartition();
        InputStream inputStream = batchCollector.getAsInputStream();
        byte[] output = new byte[11 * 1024];
        int returnValue = inputStream.read(output, 0, batchCollectorSize);
        Assertions.assertNotEquals(-1, returnValue, "Input stream is shorter than it should be.");

        byte[] trimmedArray = new byte[batchCollectorSize];
        System.arraycopy(output, 0, trimmedArray, 0, trimmedArray.length);
        assertOutputArray(batchCollectorOffsetRanges, trimmedArray);
    }

    @Test
    public void testInputStreamReadByteArrayWithMultipleArrays() throws IOException {
        BatchCollector batchCollector = createPopulatedBatchCollector();
        int batchCollectorSize = batchCollector.getSizeInBytes();
        SortedMap<Integer, BatchCollector.OffsetRange> batchCollectorOffsetRanges = batchCollector.getOffsetRangeForAllPartition();
        InputStream inputStream = batchCollector.getAsInputStream();
        byte[] outputFirst = new byte[1024];
        int readBytesFirst = inputStream.read(outputFirst, 0, outputFirst.length);
        byte[] outputSecond = new byte[1024];
        int readBytesSecond = inputStream.read(outputSecond, 0, outputSecond.length);
        byte[] outputThird = new byte[1024];
        int readBytesThird = inputStream.read(outputThird, 0, outputThird.length);
        Assertions.assertEquals(batchCollectorSize, readBytesFirst + readBytesSecond + readBytesThird);
        
        byte[] trimmedArray = new byte[batchCollectorSize];
        System.arraycopy(outputFirst, 0, trimmedArray, 0, readBytesFirst);
        System.arraycopy(outputSecond, 0, trimmedArray, readBytesFirst, readBytesSecond);
        System.arraycopy(outputThird, 0, trimmedArray, readBytesFirst + readBytesSecond, readBytesThird);
        assertOutputArray(batchCollectorOffsetRanges, trimmedArray);
    }

    private void assertOutputArray(SortedMap<Integer, BatchCollector.OffsetRange> batchCollectorOffsetRanges, byte[] output) {
        long timestamp = 0;
        // Add 1 record for partition 1,2 records for partition 2,... in inverse partition order
        for (int partition = 10; partition >= 1; partition--) {
            BatchCollector.OffsetRange offsetRange = batchCollectorOffsetRanges.get(partition);
            ByteBuffer partitionBuffer = ByteBuffer.wrap(
                    output,
                    offsetRange.startInclusive(),
                    offsetRange.endExclusive() - offsetRange.startInclusive());
            for (int i = 1; i <= partition; i++) {
                timestamp++;
                // Partition
                int recordPartition = partitionBuffer.getInt();
                assertEquals(partition, recordPartition);

                // Record key
                int recordKeyLength = partitionBuffer.getInt();
                Assertions.assertTrue(recordKeyLength >= 5);
                byte[] recordKeyBytes = new byte[recordKeyLength];
                partitionBuffer.get(recordKeyBytes);
                String recordKey = new String(recordKeyBytes, StandardCharsets.UTF_8);
                assertEquals("key_" + timestamp, recordKey);

                // Record value
                int recordValueLength = partitionBuffer.getInt();
                Assertions.assertTrue(recordValueLength >= 7);
                byte[] recordValueBytes = new byte[recordValueLength];
                partitionBuffer.get(recordValueBytes);
                String recordValue = new String(recordValueBytes, StandardCharsets.UTF_8);
                assertEquals("value_" + timestamp, recordValue);

                // Timestamp
                long recordTimestamp = partitionBuffer.getLong();
                assertEquals(timestamp, recordTimestamp);

                // Header
                int recordHeadersSize = partitionBuffer.getInt();
                assertEquals(0, recordHeadersSize);
            }
        }
    }

    private BatchCollector createPopulatedBatchCollector() {
        BatchCollector batchCollector = new BatchCollector(SMALL_MAX_BYTES_PER_PARTITION);
        long timestamp = 0;
        // Add 1 record for partition 1,2 records for partition 2,... in inverse partition order
        for (int partition = 10; partition >= 1; partition--) {
            for (int i = 1; i <= partition; i++) {
                timestamp++;
                ByteBuffer recordKey = ByteBuffer.wrap(("key_" + timestamp).getBytes(StandardCharsets.UTF_8));
                ByteBuffer recordValue = ByteBuffer.wrap(("value_" + timestamp).getBytes(StandardCharsets.UTF_8));
                RecordHeaders recordHeaders = new RecordHeaders();
                batchCollector.addRecord(partition, recordKey, recordValue, timestamp, recordHeaders);
            }
        }
        return batchCollector;
    }

}