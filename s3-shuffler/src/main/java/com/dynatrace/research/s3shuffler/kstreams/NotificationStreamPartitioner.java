package com.dynatrace.research.s3shuffler.kstreams;

import org.apache.kafka.streams.processor.StreamPartitioner;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

public class NotificationStreamPartitioner implements StreamPartitioner<NotificationDestination, NotificationDetails> {

    @Override
    public Integer partition(String topic, NotificationDestination destination, NotificationDetails value, int numPartitions) {
        return destination.partition();
    }

    @Override
    public Optional<Set<Integer>> partitions(String topic, NotificationDestination destination, NotificationDetails value, int numPartitions) {
        return Optional.of(Collections.singleton(this.partition(topic, destination, value, numPartitions)));
    }

}
