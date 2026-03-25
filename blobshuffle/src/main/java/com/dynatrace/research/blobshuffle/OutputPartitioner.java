/*
 * Copyright (c) 2026 Dynatrace LLC. All rights reserved.
 *
 * This software and associated documentation files (the "Software") are being
 * made available by Dynatrace LLC for the sole purpose of illustrating the
 * implementation of certain algorithms which are published. Permission is
 * hereby granted, free of charge, to any person obtaining a copy of the
 * Software, to view and use the Software for internal, non-production,
 * non-commercial purposes only. Without limiting the foregoing, the Software
 * may not (i) be used to process live data or train, fine-tune, enrich or
 * improve any machine learning or foundation model or other artificial
 * intelligence model or system or (ii) distributed, sublicensed, modified, used
 * to provide a service, or sold either alone or as part of or in combination
 * with any other software. The Software shall at all times be considered the
 * proprietary property of Dynatrace LLC.
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
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
