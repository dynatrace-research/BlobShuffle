package com.dynatrace.research.blobshuffle.kstreams;

import com.dynatrace.research.blobshuffle.BatchCollector;
import com.dynatrace.research.blobshuffle.NotificationDestination;
import com.dynatrace.research.blobshuffle.NotificationDetails;
import org.apache.kafka.streams.processor.api.Record;

import java.util.Iterator;
import java.util.Map;

/**
 * Groups all per-partition notifications produced by a single batch write to object storage.
 *
 * <p>After the Batcher writes a batch to object storage, it creates a {@link NotificationSet} that
 * maps each output partition to its byte range ({@link BatchCollector.OffsetRange}) within
 * the object. The set can be converted to Kafka records via {@link #asKafkaRecords()} for
 * forwarding on the notification topic.
 */
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

    /**
     * Returns the object storage path of the batch.
     *
     * @return the object path
     */
    public String getObjectPath() {
        return objectPath;
    }

    /**
     * Returns an iterable of Kafka records, one per output partition contained in this batch.
     * Each record key is a {@link NotificationDestination} and the value is a
     * {@link NotificationDetails} describing the byte range for that partition.
     *
     * @return the notification records
     */
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
