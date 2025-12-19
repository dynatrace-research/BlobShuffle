package com.dynatrace.research.s3shuffler;

import com.dynatrace.hash4j.hashing.Hasher64;
import com.dynatrace.hash4j.hashing.Hashing;
import org.apache.kafka.clients.producer.internals.BuiltInPartitioner;
import org.apache.kafka.common.utils.Utils;

import java.util.*;

public class OutputPartitioner {

    private final Partitioner partitioner;

    private Map<Integer,String> partitionsToRack;

    private final Hasher64 hasher = Hashing.komihash4_3(0x81c1f2b68968424dL);

    public OutputPartitioner(Partitioner partitioner, Map<Integer,String> partitionsToRack) {
        this.partitioner = partitioner;
        this.partitionsToRack = partitionsToRack;
    }

    public int getPartitionForRecordKey(byte[] recordKey) {
        int numPartitions = partitionsToRack.size();
        return this.partitioner.getPartitionForRecordKey(recordKey, numPartitions);
    }

    public String getRackForPartition(int partition) {
        return partitionsToRack.get(partition);
    }

    public String getRackForRecordKey(byte[] recordKey) {
        int partition = getPartitionForRecordKey(recordKey);
        return getRackForPartition(partition);
    }

    public int getNumberOfPartitions() {
        return this.partitionsToRack.size();
    }

    public void setOutputPartitionsToRack(Map<Integer, String> partitionsToRack) {
        this.partitionsToRack = partitionsToRack;
    }
}
