package com.dynatrace.research.blobshuffle;

import com.dynatrace.research.blobshuffle.kstreams.NotificationStreamPartitioner;

/**
 * Identifies the intended recipient of a batch notification.
 * Used as the Kafka record key on the notification topic so that
 * {@link NotificationStreamPartitioner} can route each notification to the correct
 * Debatcher partition.
 *
 * @param destination the target availability zone (rack) identifier
 * @param partition   the target output partition number
 */
public record NotificationDestination(String destination, int partition) {
}
