package com.dynatrace.research.blobshuffle;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.*;

import static org.assertj.core.api.Assertions.assertThat;

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

    @Test
    public void testWriteFallbackOnForwardFailure() throws IOException {
        S3BatchFacade localFacade = Mockito.mock();

        AtomicReference<byte[]> capturedBytes = new AtomicReference<>(null);
        Mockito.doAnswer(invocation -> {
            InputStream is = invocation.getArgument(1);
            capturedBytes.set(is.readAllBytes());
            return null;
        }).when(localFacade).writeBatch(anyString(), any(), anyInt());

        // "test2" routes to the second member (localhost:7071), which has no server — triggers fallback
        try (ProxyingS3BatchFacade facade = new ProxyingS3BatchFacade(
                localFacade,
                7070,
                new StaticMemberDiscoverer("localhost:7070", "localhost:7071"),
                new SimpleMeterRegistry())) {

            BatchCollector batchCollector = new BatchCollector(1024 * 1024);
            batchCollector.addRecord(0, new byte[]{1, 2, 3}, new byte[]{4, 5, 6}, 0L, new RecordHeaders());
            int batchSize = batchCollector.getSizeInBytes(); // must be called before getAsInputStream() flips the buffers
            InputStream batchInputStream = batchCollector.getAsInputStream();

            // Should not throw - falls back to localFacade after forward failure
            facade.writeBatch("test2", batchInputStream, batchSize);

            Mockito.verify(localFacade).writeBatch(eq("test2"), any(InputStream.class), eq(batchSize));
            assertThat(capturedBytes.get()).hasSize(batchSize);
        }
    }

    @Test
    public void testReadFallbackOnForwardFailure() throws IOException {
        S3BatchFacade localFacade = Mockito.mock();

        byte[] testBatch = new byte[]{1, 2, 3, 4, 5};
        Mockito.when(localFacade.readBatch("test2"))
                .thenReturn(new S3BatchFacade.SizeAwareInputStream(new ByteArrayInputStream(testBatch), testBatch.length));

        // "test2" routes to the second member (localhost:7071), which has no server — triggers fallback
        try (ProxyingS3BatchFacade facade = new ProxyingS3BatchFacade(
                localFacade,
                7070,
                new StaticMemberDiscoverer("localhost:7070", "localhost:7071"),
                new SimpleMeterRegistry())) {

            S3BatchFacade.SizeAwareInputStream result = facade.readBatch("test2");

            assertThat(result.readAllBytes()).isEqualTo(testBatch);
            Mockito.verify(localFacade).readBatch("test2");
        }
    }

    @Test
    public void testBatchCollectorInputStreamRewind() throws IOException {
        BatchCollector batchCollector = new BatchCollector(1024 * 1024);
        batchCollector.addRecord(0, new byte[]{1, 2, 3}, new byte[]{4, 5, 6}, 42L, new RecordHeaders());
        batchCollector.addRecord(1, new byte[]{7, 8}, new byte[]{9}, 99L, new RecordHeaders());

        InputStream stream = batchCollector.getAsInputStream();
        byte[] firstRead = stream.readAllBytes();

        assertThat(firstRead).hasSize(batchCollector.getSizeInBytes());

        ((RewindableInputStream) stream).rewind();
        byte[] secondRead = stream.readAllBytes();

        assertThat(secondRead).isEqualTo(firstRead);
    }
}