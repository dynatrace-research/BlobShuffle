package com.dynatrace.research.s3shuffler;

import java.net.http.HttpRequest;
import java.nio.ByteBuffer;
import java.util.concurrent.Flow;

/**
 * An {@link HttpRequest.BodyPublisher} wrapper that reports a pre-known content length,
 * enabling the HTTP client to set a {@code Content-Length} header.
 */
public class SizeAwareBodyPublisher implements HttpRequest.BodyPublisher {

    private final HttpRequest.BodyPublisher wrappedBodyPublisher;

    private final long contentLength;

    /**
     * Creates a new size-aware body publisher.
     *
     * @param bodyPublisher the underlying body publisher
     * @param contentLength the content length in bytes
     */
    public SizeAwareBodyPublisher(HttpRequest.BodyPublisher bodyPublisher, long contentLength) {
        this.wrappedBodyPublisher = bodyPublisher;
        this.contentLength = contentLength;
    }

    @Override
    public long contentLength() {
        return this.contentLength;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super ByteBuffer> subscriber) {
        this.wrappedBodyPublisher.subscribe(subscriber);
    }


}
