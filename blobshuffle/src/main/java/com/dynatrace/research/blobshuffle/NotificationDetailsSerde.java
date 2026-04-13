package com.dynatrace.research.blobshuffle;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Kafka {@link Serde} for {@link NotificationDetails}, encoding the object path, start
 * offset, and end offset as a compact binary format.
 */
public class NotificationDetailsSerde implements Serde<NotificationDetails> {

    @Override
    public Serializer<NotificationDetails> serializer() {
        return new NotificationDetailsSerializer();
    }

    @Override
    public Deserializer<NotificationDetails> deserializer() {
        return new NotificationDetailsDeserializer();
    }

    /** Serializes a {@link NotificationDetails} to bytes. */
    public static class NotificationDetailsSerializer implements Serializer<NotificationDetails> {

        @Override
        public byte[] serialize(String topic, NotificationDetails notificationDetails) {
            byte[] objectPathBytes = notificationDetails.objectPath().getBytes(StandardCharsets.UTF_8);
            int offsetStartInclusive = notificationDetails.offsetStartInclusive();
            int offsetEndExclusive = notificationDetails.offsetEndExclusive();
            ByteBuffer buffer = ByteBuffer.wrap(new byte[Integer.BYTES + objectPathBytes.length + Integer.BYTES + Integer.BYTES]);
            buffer.putInt(objectPathBytes.length);
            buffer.put(objectPathBytes);
            buffer.putInt(offsetStartInclusive);
            buffer.putInt(offsetEndExclusive);
            return buffer.array();
        }

    }

    /** Deserializes a {@link NotificationDetails} from bytes. */
    public static class NotificationDetailsDeserializer implements Deserializer<NotificationDetails> {

        @Override
        public NotificationDetails deserialize(String topic, byte[] bytes) {
            final ByteBuffer buffer = ByteBuffer.wrap(bytes);
            int objectPathBytesLength = buffer.getInt();
            byte[] objectPathBytes = new byte[objectPathBytesLength];
            buffer.get(objectPathBytes);
            String objectPath = new String(objectPathBytes, StandardCharsets.UTF_8);
            int offsetStartInclusive = buffer.getInt();
            int offsetEndExclusive = buffer.getInt();
            return new NotificationDetails(objectPath, offsetStartInclusive, offsetEndExclusive);
        }

    }

}
