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
package com.dynatrace.research.s3shuffler;

import com.dynatrace.hash4j.hashing.Hasher64;
import com.dynatrace.hash4j.hashing.Hashing;
import com.dynatrace.research.s3shuffler.util.Kafka4Dot2Murmur2;
import com.dynatrace.research.s3shuffler.util.KafkaImprovedMurmur2;
import org.apache.kafka.clients.producer.internals.BuiltInPartitioner;
import org.apache.kafka.common.utils.Utils;

import java.util.SplittableRandom;

/**
 * Functional interface for mapping serialized record keys to partition numbers.
 */
@FunctionalInterface
public interface Partitioner {

    /** Kafka's built-in partitioner. */
    Partitioner KAFKA_BUILTIN = BuiltInPartitioner::partitionForKey;

    /** Murmur2 partitioner matching Kafka 4.2+ behavior. */
    Partitioner KAFKA_4_2 = (recordKey, numPartitions) ->  {
        int keyHash = Kafka4Dot2Murmur2.murmur2(recordKey);
        return Utils.toPositive(keyHash) % numPartitions;
    };

    /** Optimized Murmur2 partitioner. */
    Partitioner KAFKA_OPTIMIZED = (recordKey, numPartitions) ->  {
        int keyHash = KafkaImprovedMurmur2.murmur2(recordKey);
        return Utils.toPositive(keyHash) % numPartitions;
    };

    /** Komihash 4.3 partitioner. */
    Partitioner KOMIHASH4_3 = new Partitioner() {

        private final Hasher64 hasher = Hashing.komihash4_3(0x81c1f2b68968424dL);

        @Override
        public int getPartitionForRecordKey(byte[] recordKey, int numPartitions) {
            int keyHash = this.hasher.hashBytesToInt(recordKey, 0, recordKey.length);
            return Utils.toPositive(keyHash) % numPartitions;
        }
    };

    /** Random partitioner (primarily for testing). */
    Partitioner RANDOM = new Partitioner() {

        private final SplittableRandom random = new SplittableRandom(0x81c1f2b68968424dL);

        @Override
        public int getPartitionForRecordKey(byte[] recordKey, int numPartitions) {
            return random.nextInt(numPartitions);
        }
    };

    /**
     * Returns the partitioner for the given name.
     *
     * @param partitioner the partitioner name (one of {@code KAFKA_BUILTIN}, {@code KAFKA_4_2},
     *                    {@code KAFKA_OPTIMIZED}, {@code KOMIHASH4_3}, {@code RANDOM})
     * @return the corresponding partitioner
     * @throws IllegalArgumentException if no partitioner with the given name exists
     */
    static Partitioner of(String partitioner) {
        return switch (partitioner) {
            case "KAFKA_BUILTIN" -> KAFKA_BUILTIN;
            case "KAFKA_4_2" -> KAFKA_4_2;
            case "KAFKA_OPTIMIZED" -> KAFKA_OPTIMIZED;
            case "KOMIHASH4_3" -> KOMIHASH4_3;
            case "RANDOM" -> RANDOM;
            default -> throw new IllegalArgumentException("No partitioner " + partitioner + " exists");
        };
    }

    /**
     * Computes the partition number for the given serialized record key.
     *
     * @param recordKey the serialized record key
     * @param numPartitions the total number of partitions
     * @return the partition number (0-based)
     */
    int getPartitionForRecordKey(byte[] recordKey, int numPartitions);

}
