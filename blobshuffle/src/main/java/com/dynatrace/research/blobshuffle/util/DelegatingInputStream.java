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
package com.dynatrace.research.blobshuffle.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * An {@link InputStream} decorator that forwards all operations to a wrapped delegate.
 * Extend this class to override only the relevant methods.
 */
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
