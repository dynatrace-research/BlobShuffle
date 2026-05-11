package com.dynatrace.research.blobshuffle;

import java.io.InputStream;

/**
 * An {@link InputStream} that can be rewound to the beginning, allowing the data to be read again
 * from the start. Compared to the {@link InputStream#mark(int)} / {@link InputStream#reset()}
 * mechanism, this class requires no setup before reading, avoids the awkward {@code readlimit}
 * parameter, exposes rewindability as a compile-time type distinction rather than a runtime
 * {@link InputStream#markSupported()} check, and declares no checked exception.
 */
public abstract class RewindableInputStream extends InputStream {

    /**
     * Rewinds this stream to the beginning so that the data can be read again from the start.
     */
    public abstract void rewind();

}
