package com.dynatrace.research.s3shuffler.util;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

/**
 * The improved Murmur2 implementation from <a href="https://github.com/apache/kafka/pull/20359">https://github.com/apache/kafka/pull/20359</a>.
 */
public final class KafkaImprovedMurmur2 {

    private KafkaImprovedMurmur2() {}

    private static final VarHandle INT_HANDLE =
            MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);

    /**
     * Generates 32 bit murmur2 hash from byte array
     * @param data byte array to hash
     * @return 32 bit hash of the given array
     */
    public static int murmur2(final byte[] data) {
        final int length = data.length;
        int seed = 0x9747b28c;
        // 'm' and 'r' are mixing constants generated offline.
        // They're not really 'magic', they just happen to work well.
        final int m = 0x5bd1e995;
        final int r = 24;

        // Initialize the hash to a random value
        int h = seed ^ length;

        if (length < 4) {
            if (length != 0) {
                h ^= data[0] & 0xff;
                if (length != 1) {
                    h ^= (data[1] & 0xff) << 8;
                    if (length != 2) h ^= (data[2] & 0xff) << 16;
                }
                h *= m;
            }
            h ^= h >>> 13;
            h *= m;
            h ^= h >>> 15;
            return h;
        }

        for (int i = 0; i <= length - 4; i += 4) {
            int k = (int) INT_HANDLE.get(data, i);
            k *= m;
            k ^= k >>> r;
            k *= m;
            h *= m;
            h ^= k;
        }

        // Handle the last few bytes of the input array
        if ((length & 3) != 0) {
            h ^= (int) INT_HANDLE.get(data, length - 4) >>> -(length << 3);
            h *= m;
        }
        h ^= h >>> 13;
        h *= m;
        h ^= h >>> 15;

        return h;
    }

}