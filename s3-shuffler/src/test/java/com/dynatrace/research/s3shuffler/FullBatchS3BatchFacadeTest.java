package com.dynatrace.research.s3shuffler;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.*;

class FullBatchS3BatchFacadeTest {

    @Test
    public void testPartialInputStreamComplete() throws IOException {
        byte[] sourceBytes = new byte[]{1, 2, 3, 4, 5};
        S3BatchFacade.SizeAwareInputStream innerStream = new S3BatchFacade.SizeAwareInputStream(
                new ByteArrayInputStream(sourceBytes), sourceBytes.length);
        InputStream partialStream = new FullBatchS3BatchFacade.PartialSizeAwareInputStream(innerStream, 0, sourceBytes.length);
        int i = 0;
        while (true) {
            int readByte = partialStream.read();
            if (readByte == -1) {
                break;
            }
            assertEquals(sourceBytes[i], (byte) readByte);
            i++;
        }
        assertEquals(sourceBytes.length, i);
    }

    @Test
    public void testPartialInputStreamStart() throws IOException {
        byte[] sourceBytes = new byte[]{1, 2, 3, 4, 5};
        S3BatchFacade.SizeAwareInputStream innerStream = new S3BatchFacade.SizeAwareInputStream(
                new ByteArrayInputStream(sourceBytes), 5);
        InputStream partialStream = new FullBatchS3BatchFacade.PartialSizeAwareInputStream(innerStream, 0, 3);
        int i = 0;
        while (true) {
            int readByte = partialStream.read();
            if (readByte == -1) {
                break;
            }
            assertEquals(sourceBytes[i], (byte) readByte);
            i++;
        }
        assertEquals(3, i);
    }

    @Test
    public void testPartialInputStreamEnd() throws IOException {
        byte[] sourceBytes = new byte[]{1, 2, 3, 4, 5};
        S3BatchFacade.SizeAwareInputStream innerStream = new S3BatchFacade.SizeAwareInputStream(
                new ByteArrayInputStream(sourceBytes), 5);
        InputStream partialStream = new FullBatchS3BatchFacade.PartialSizeAwareInputStream(innerStream, 2, 5);
        int i = 0;
        while (true) {
            int readByte = partialStream.read();
            if (readByte == -1) {
                break;
            }
            assertEquals(sourceBytes[i + 2], (byte) readByte);
            i++;
        }
        assertEquals(3, i);
    }

}