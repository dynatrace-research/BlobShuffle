package com.dynatrace.research.blobshuffle;

import java.io.ByteArrayInputStream;

/**
 * A {@link RewindableInputStream} backed by a byte array.
 */
public final class RewindableByteArrayInputStream extends RewindableInputStream {

    private final ByteArrayInputStream delegate;

    public RewindableByteArrayInputStream(byte[] buf) {
        this.delegate = new ByteArrayInputStream(buf);
    }

    @Override
    public void rewind() {
        this.delegate.reset(); // resets to position 0 (default mark)
    }

    @Override
    public int read() {
        return this.delegate.read();
    }

    @Override
    public int read(byte[] b, int off, int len) {
        return this.delegate.read(b, off, len);
    }

    @Override
    public int available() {
        return this.delegate.available();
    }

    @Override
    public long skip(long n) {
        return this.delegate.skip(n);
    }
}
