package com.dynatrace.research.blobshuffle.kstreams;

import com.dynatrace.research.blobshuffle.BatchCollector;
import org.apache.kafka.streams.processor.api.Record;

import java.util.Iterator;
import java.util.Map;

public class NotificationSet {

    private final String destination;

    private final String objectPath;

    private final long batchTimestamp;

    private final Map<Integer, BatchCollector.OffsetRange> partitionsToOffsetRanges;

    public NotificationSet(String destination, String objectPath, Map<Integer, BatchCollector.OffsetRange> partitionsToOffsetRanges, long batchTimestamp) {
        this.destination = destination;
        this.objectPath = objectPath;
        this.partitionsToOffsetRanges = partitionsToOffsetRanges;
        this.batchTimestamp = batchTimestamp;
    }

    public String getObjectPath() {
        return objectPath;
    }

    public Iterable<Record<NotificationDestination, NotificationDetails>> asKafkaRecords() {
        return () -> new Iterator<>() {

            private final Iterator<Map.Entry<Integer, BatchCollector.OffsetRange>> partitionsToOffsetRangesIterator = partitionsToOffsetRanges.entrySet().iterator();

            @Override
            public boolean hasNext() {
                return partitionsToOffsetRangesIterator.hasNext();
            }

            @Override
            public Record<NotificationDestination, NotificationDetails> next() {
                Map.Entry<Integer, BatchCollector.OffsetRange> integerOffsetRangeEntry = partitionsToOffsetRangesIterator.next();
                int partition = integerOffsetRangeEntry.getKey();
                BatchCollector.OffsetRange offsetRange = integerOffsetRangeEntry.getValue();
                return new Record<>(
                        new NotificationDestination(destination, partition),
                        new NotificationDetails(objectPath, offsetRange.startInclusive(), offsetRange.endExclusive()),
                        batchTimestamp);
            }
        };
    }

}
