package com.dynatrace.research.blobshuffle.standalone;

import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.Closeable;

/**
 * Interface for a producer that transparently batches records to an object storage service and sends lightweight
 * notifications through Kafka. This provides the same cost-saving object storage offloading behavior as
 * the Kafka Streams BlobShuffle, but works with the plain Kafka Producer API.
 *
 * @param <K> the record key type
 * @param <V> the record value type
 */
public interface BlobShuffleProducer<K, V> extends Closeable {

    /**
     * Sends a record. The record is serialized and added to an internal batch collector.
     * When the batch reaches the desired size or the maximum duration has elapsed, the
     * batch is asynchronously written to object storage and notification records are sent to Kafka.
     *
     * @param record the producer record to send
     */
    void send(ProducerRecord<K, V> record);

    /**
     * Flushes all pending batches to object storage and blocks until all notifications have been
     * acknowledged by Kafka.
     */
    void flush();

    @Override
    void close();
}
