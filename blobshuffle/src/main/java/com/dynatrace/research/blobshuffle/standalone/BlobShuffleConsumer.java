package com.dynatrace.research.blobshuffle.standalone;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collection;
import java.util.List;

/**
 * Interface for a consumer that reads notification records from Kafka, fetches the corresponding
 * record batches from object storage, and returns the deserialized original records. This provides the same
 * cost-saving object storage offloading behavior as the Kafka Streams BlobShuffle, but works with the plain
 * Kafka Consumer API.
 *
 * @param <K> the record key type
 * @param <V> the record value type
 */
public interface BlobShuffleConsumer<K, V> extends Closeable {

    /**
     * Subscribe to the given notification topics.
     *
     * @param topics the notification topic(s) to subscribe to
     */
    void subscribe(Collection<String> topics);

    /**
     * Polls for new records. This method polls the inner Kafka consumer for notification
     * records, schedules async object storage batch fetches, and returns all deserialized records
     * from completed fetches.
     *
     * @param timeout the maximum time to block waiting for notification records
     * @return the deserialized records from completed batch fetches
     */
    List<ConsumerRecord<K, V>> poll(Duration timeout);

    /**
     * Commits the current consumer offsets synchronously.
     * Should only be called after successfully processing records returned by {@link #poll(Duration)}.
     */
    void commitSync();

    /**
     * Commits the current consumer offsets asynchronously.
     */
    void commitAsync();

    @Override
    void close();
}
