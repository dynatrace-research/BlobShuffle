package com.dynatrace.research.blobshuffle.util;

import java.nio.ByteBuffer;

public class ByteBuffers {

    private ByteBuffers(){}

    public static ByteBuffer wrapNullable(byte[] array) {
        return (array != null) ? ByteBuffer.wrap(array) : null;
    }

    public static ByteBuffer wrapNullable(byte[] array, int offset, int length) {
        return (array != null) ? ByteBuffer.wrap(array, offset, length) : null;
    }

}
