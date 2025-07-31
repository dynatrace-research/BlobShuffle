package com.dynatrace.research.s3shuffler.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class DelegatingInputStream extends InputStream {

    private final InputStream delegate;

    public DelegatingInputStream(InputStream delegate) {
        super();
        this.delegate = delegate;
    }

    @Override
    public int read() throws IOException {
        return this.delegate.read();
    }

    @Override
    public int read(byte[] b) throws IOException {
        return this.delegate.read(b);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return this.delegate.read(b, off, len);
    }

    @Override
    public byte[] readAllBytes() throws IOException {
        return this.delegate.readAllBytes();
    }

    @Override
    public byte[] readNBytes(int len) throws IOException {
        return this.delegate.readNBytes(len);
    }

    @Override
    public int readNBytes(byte[] b, int off, int len) throws IOException {
        return this.delegate.readNBytes(b, off, len);
    }

    @Override
    public long skip(long n) throws IOException {
        return this.delegate.skip(n);
    }

    @Override
    public void skipNBytes(long n) throws IOException {
        this.delegate.skipNBytes(n);
    }

    @Override
    public int available() throws IOException {
        return this.delegate.available();
    }

    @Override
    public void close() throws IOException {
        this.delegate.close();
    }

    @Override
    public synchronized void mark(int readlimit) {
        this.delegate.mark(readlimit);
    }

    @Override
    public synchronized void reset() throws IOException {
        this.delegate.reset();
    }

    @Override
    public boolean markSupported() {
        return this.delegate.markSupported();
    }

    @Override
    public long transferTo(OutputStream out) throws IOException {
        return this.delegate.transferTo(out);
    }
}
