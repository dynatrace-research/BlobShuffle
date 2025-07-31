package com.dynatrace.research.s3shuffler;

import org.apache.kafka.common.utils.Utils;

import java.util.*;

public class OutputPartitioner {

    private Map<Integer,String> partitionsToRack;

    public OutputPartitioner(Map<Integer,String> partitionsToRack) {
        this.partitionsToRack = partitionsToRack;
    }

    public int getPartitionForRecordHash(int recordHash) {
        int numPartitions = partitionsToRack.size();
        return Utils.toPositive(recordHash) % numPartitions;
    }

    public String getRackForPartition(int partition) {
        return partitionsToRack.get(partition);
    }

    public String getRackForRecordHash(int recordHash) {
        int partition = getPartitionForRecordHash(recordHash);
        return getRackForPartition(partition);
    }

    public void setOutputPartitionsToRack(Map<Integer, String> partitionsToRack) {
        this.partitionsToRack = partitionsToRack;
    }
}
