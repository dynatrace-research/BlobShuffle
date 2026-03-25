package com.dynatrace.research.blobshuffle;

import com.dynatrace.hash4j.hashing.Hasher64;
import com.dynatrace.hash4j.hashing.Hashing;
import org.apache.kafka.clients.producer.internals.BuiltInPartitioner;
import org.apache.kafka.common.utils.Utils;

import java.util.*;

/**
 * Maps serialized record keys to output partitions and tracks the partition-to-rack assignment.
 */
public class OutputPartitioner {

    private final Partitioner partitioner;

    private Map<Integer,String> partitionsToRack;

    private final Hasher64 hasher = Hashing.komihash4_3(0x81c1f2b68968424dL);

    /**
     * Creates a new output partitioner.
     *
     * @param partitioner the partitioning function
     * @param partitionsToRack initial mapping of partition numbers to rack (AZ) identifiers
     */
    public OutputPartitioner(Partitioner partitioner, Map<Integer,String> partitionsToRack) {
        this.partitioner = partitioner;
        this.partitionsToRack = partitionsToRack;
    }

    /**
     * Returns the partition number for the given serialized record key.
     *
     * @param recordKey the serialized record key
     * @return the partition number
     */
    public int getPartitionForRecordKey(byte[] recordKey) {
        int numPartitions = partitionsToRack.size();
        return this.partitioner.getPartitionForRecordKey(recordKey, numPartitions);
    }

    /**
     * Returns the rack (AZ) identifier for the given partition.
     *
     * @param partition the partition number
     * @return the rack identifier, or {@code null} if not known
     */
    public String getRackForPartition(int partition) {
        return partitionsToRack.get(partition);
    }

    /**
     * Returns the rack (AZ) identifier for the given serialized record key.
     *
     * @param recordKey the serialized record key
     * @return the rack identifier
     */
    public String getRackForRecordKey(byte[] recordKey) {
        int partition = getPartitionForRecordKey(recordKey);
        return getRackForPartition(partition);
    }

    /**
     * Returns the total number of output partitions.
     *
     * @return the partition count
     */
    public int getNumberOfPartitions() {
        return this.partitionsToRack.size();
    }

    /**
     * Updates the partition-to-rack mapping, for example after refreshing from Kafka metadata.
     *
     * @param partitionsToRack the new mapping
     */
    public void setOutputPartitionsToRack(Map<Integer, String> partitionsToRack) {
        this.partitionsToRack = partitionsToRack;
    }
}
