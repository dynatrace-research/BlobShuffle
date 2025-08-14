package com.dynatrace.research.s3shuffler;

import com.dynatrace.hash4j.hashing.Hasher64;
import com.dynatrace.hash4j.hashing.Hashing;
import org.apache.kafka.clients.producer.internals.BuiltInPartitioner;
import org.apache.kafka.common.utils.Utils;

import java.util.*;

public class OutputPartitioner {

    private Map<Integer,String> partitionsToRack;

    private final Hasher64 hasher = Hashing.komihash4_3(0x81c1f2b68968424dL);

    public OutputPartitioner(Map<Integer,String> partitionsToRack) {
        this.partitionsToRack = partitionsToRack;
    }

    public int getPartitionForRecordKey(byte[] recordKey) {
        int numPartitions = partitionsToRack.size();
        //return BuiltInPartitioner.partitionForKey(recordKey, numPartitions);
        int keyHash = this.hasher.hashBytesToInt(recordKey, 0, recordKey.length);
        return Utils.toPositive(keyHash) % numPartitions;
    }

    public String getRackForPartition(int partition) {
        return partitionsToRack.get(partition);
    }

    public String getRackForRecordKey(byte[] recordKey) {
        int partition = getPartitionForRecordKey(recordKey);
        return getRackForPartition(partition);
    }

    public void setOutputPartitionsToRack(Map<Integer, String> partitionsToRack) {
        this.partitionsToRack = partitionsToRack;
    }
}
