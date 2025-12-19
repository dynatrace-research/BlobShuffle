package com.dynatrace.research.s3shuffler;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.*;

class ProxyingS3BatchFacadeTest {

    @Test
    public void testWriteForwardToSameFacade() throws IOException {
        S3BatchFacade s3BatchFacade1 = Mockito.mock();
        S3BatchFacade s3BatchFacade2 = Mockito.mock();

        // Capture the content from call to s3BatchFacade1.s3BatchFacade2(..)
        AtomicReference<byte[]> capturedBytes = new AtomicReference<>(null);
        Mockito.doAnswer(invocation -> {
            InputStream inputStream = invocation.getArgument(1);
            capturedBytes.set(inputStream.readAllBytes());
            return null;
        }).when(s3BatchFacade1).writeBatch(anyString(), any(), anyInt());

        try (ProxyingS3BatchFacade proxyingS3BatchFacade1 = new ProxyingS3BatchFacade(
                s3BatchFacade1,
                7070,
                new StaticMemberDiscoverer("localhost:7070", "localhost:7071"),
                new SimpleMeterRegistry());
             ProxyingS3BatchFacade proxyingS3BatchFacade2 = new ProxyingS3BatchFacade(
                     s3BatchFacade2,
                     7071,
                     new StaticMemberDiscoverer("localhost:7071", "localhost:7070"),
                     new SimpleMeterRegistry())) {

            byte[] testBatch = new byte[16 * 1024 * 1024];
            proxyingS3BatchFacade1.writeBatch("test1", new ByteArrayInputStream(testBatch), testBatch.length);

            // "test1" is hashed such that the call goes to s3BatchFacade1
            // TODO improve this by stubbing the hash/modulo function
            Mockito.verify(s3BatchFacade1).writeBatch(eq("test1"), any(InputStream.class), eq(testBatch.length));
            Assertions.assertArrayEquals(testBatch, capturedBytes.get());
            Mockito.verifyNoInteractions(s3BatchFacade2);
        }
    }

    @Test
    public void testWriteForwardToOtherFacade() throws IOException {
        S3BatchFacade s3BatchFacade1 = Mockito.mock();
        S3BatchFacade s3BatchFacade2 = Mockito.mock();

        // Capture the content from call to s3BatchFacade1.s3BatchFacade2(..)
        AtomicReference<byte[]> capturedBytes = new AtomicReference<>(null);
        Mockito.doAnswer(invocation -> {
            InputStream inputStream = invocation.getArgument(1);
            capturedBytes.set(inputStream.readAllBytes());
            return null;
        }).when(s3BatchFacade2).writeBatch(anyString(), any(), anyInt());

        try (ProxyingS3BatchFacade proxyingS3BatchFacade1 = new ProxyingS3BatchFacade(
                s3BatchFacade1,
                7070,
                new StaticMemberDiscoverer("localhost:7070", "localhost:7071"),
                new SimpleMeterRegistry());
             ProxyingS3BatchFacade proxyingS3BatchFacade2 = new ProxyingS3BatchFacade(
                 s3BatchFacade2,
                 7071,
                 new StaticMemberDiscoverer("localhost:7071", "localhost:7070"),
                 new SimpleMeterRegistry())) {

            byte[] testBatch = new byte[16 * 1024 * 1024];
            proxyingS3BatchFacade1.writeBatch("test2", new ByteArrayInputStream(testBatch), testBatch.length);

            // "test2" is hashed such that the call goes to s3BatchFacade2
            // TODO improve this by stubbing the hash/modulo function
            Mockito.verifyNoInteractions(s3BatchFacade1);
            Mockito.verify(s3BatchFacade2).writeBatch(eq("test2"), any(InputStream.class), eq(testBatch.length));
            Assertions.assertArrayEquals(testBatch, capturedBytes.get());
        }
    }

    @Test
    public void testReadForwardToSameFacade() throws IOException {
        S3BatchFacade s3BatchFacade1 = Mockito.mock();
        S3BatchFacade s3BatchFacade2 = Mockito.mock();

        byte[] testBatch = new byte[16 * 1024 * 1024];
        Mockito.when(s3BatchFacade1.readBatch(any()))
                .thenReturn(new S3BatchFacade.SizeAwareInputStream(new ByteArrayInputStream(testBatch), testBatch.length));

        try (ProxyingS3BatchFacade proxyingS3BatchFacade1 = new ProxyingS3BatchFacade(
                s3BatchFacade1,
                7070,
                new StaticMemberDiscoverer("localhost:7070", "localhost:7071"),
                new SimpleMeterRegistry());
             ProxyingS3BatchFacade proxyingS3BatchFacade2 = new ProxyingS3BatchFacade(
                     s3BatchFacade2,
                     7071,
                     new StaticMemberDiscoverer("localhost:7071", "localhost:7070"),
                     new SimpleMeterRegistry())) {

            S3BatchFacade.SizeAwareInputStream readBatch = proxyingS3BatchFacade1.readBatch("test1");
            byte[] readBatchBytes = readBatch.readAllBytes();

            // "test1" is hashed such that the call goes to s3BatchFacade1
            // TODO improve this by stubbing the hash/modulo function
            Assertions.assertArrayEquals(testBatch, readBatchBytes);
            Assertions.assertEquals(testBatch.length, readBatch.size());
        }
    }

    @Test
    public void testReadForwardToOtherFacade() throws IOException {
        S3BatchFacade s3BatchFacade1 = Mockito.mock();
        S3BatchFacade s3BatchFacade2 = Mockito.mock();

        byte[] testBatch = new byte[16 * 1024 * 1024];
        Mockito.when(s3BatchFacade2.readBatch(any()))
                .thenReturn(new S3BatchFacade.SizeAwareInputStream(new ByteArrayInputStream(testBatch), testBatch.length));

        try (ProxyingS3BatchFacade proxyingS3BatchFacade1 = new ProxyingS3BatchFacade(
                s3BatchFacade1,
                7070,
                new StaticMemberDiscoverer("localhost:7070", "localhost:7071"),
                new SimpleMeterRegistry());
             ProxyingS3BatchFacade proxyingS3BatchFacade2 = new ProxyingS3BatchFacade(
                     s3BatchFacade2,
                     7071,
                     new StaticMemberDiscoverer("localhost:7071", "localhost:7070"),
                     new SimpleMeterRegistry())) {

            S3BatchFacade.SizeAwareInputStream readBatch = proxyingS3BatchFacade1.readBatch("test2");
            byte[] readBatchBytes = readBatch.readAllBytes();

            // "test1" is hashed such that the call goes to s3BatchFacade1
            // TODO improve this by stubbing the hash/modulo function
            Assertions.assertArrayEquals(testBatch, readBatchBytes);
            Assertions.assertEquals(testBatch.length, readBatch.size());
        }
    }
}