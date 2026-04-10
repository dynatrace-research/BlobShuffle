package com.dynatrace.research.blobshuffle.util;

import java.nio.ByteBuffer;

/**
 * Utility methods for working with {@link java.nio.ByteBuffer ByteBuffers}.
 */
public class ByteBuffers {

    private ByteBuffers(){}

    /**
     * Wraps a nullable byte array in a {@link java.nio.ByteBuffer}.
     *
     * @param array the array to wrap, may be {@code null}
     * @return a {@link java.nio.ByteBuffer} wrapping the array, or {@code null} if the array is {@code null}
     */
    public static ByteBuffer wrapNullable(byte[] array) {
        return (array != null) ? ByteBuffer.wrap(array) : null;
    }

    /**
     * Wraps a nullable byte array slice in a {@link java.nio.ByteBuffer}.
     *
     * @param array  the array to wrap, may be {@code null}
     * @param offset start offset in the array
     * @param length number of bytes
     * @return a {@link java.nio.ByteBuffer} wrapping the slice, or {@code null} if the array is {@code null}
     */
    public static ByteBuffer wrapNullable(byte[] array, int offset, int length) {
        return (array != null) ? ByteBuffer.wrap(array, offset, length) : null;
    }

}
