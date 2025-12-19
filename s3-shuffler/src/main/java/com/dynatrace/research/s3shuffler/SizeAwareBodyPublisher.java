package com.dynatrace.research.s3shuffler;

import java.net.http.HttpRequest;
import java.nio.ByteBuffer;
import java.util.concurrent.Flow;

public class SizeAwareBodyPublisher implements HttpRequest.BodyPublisher {

    private final HttpRequest.BodyPublisher wrappedBodyPublisher;

    private final long contentLength;

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
