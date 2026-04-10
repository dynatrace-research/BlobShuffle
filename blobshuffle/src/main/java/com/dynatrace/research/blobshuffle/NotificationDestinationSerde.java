package com.dynatrace.research.blobshuffle;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Kafka {@link Serde} for {@link NotificationDestination}, encoding the destination
 * zone and partition number as a compact binary format.
 */
public class NotificationDestinationSerde implements Serde<NotificationDestination> {

    @Override
    public Serializer<NotificationDestination> serializer() {
        return new NotificationDestinationSerializer();
    }

    @Override
    public Deserializer<NotificationDestination> deserializer() {
        return new NotificationDestinationDeserializer();
    }

    /** Serializes a {@link NotificationDestination} to bytes. */
    public static class NotificationDestinationSerializer implements Serializer<NotificationDestination> {

        @Override
        public byte[] serialize(String topic, NotificationDestination notificationDestination) {
            byte[] destinationBytes = notificationDestination.destination().getBytes(StandardCharsets.UTF_8);
            int partition = notificationDestination.partition();
            ByteBuffer buffer = ByteBuffer.wrap(new byte[Integer.BYTES + destinationBytes.length + Integer.BYTES]);
            buffer.putInt(destinationBytes.length);
            buffer.put(destinationBytes);
            buffer.putInt(partition);
            return buffer.array();
        }

    }

    /** Deserializes a {@link NotificationDestination} from bytes. */
    public static class NotificationDestinationDeserializer implements Deserializer<NotificationDestination> {

        @Override
        public NotificationDestination deserialize(String topic, byte[] bytes) {
            final ByteBuffer buffer = ByteBuffer.wrap(bytes);
            int destinationBytesLength = buffer.getInt();
            byte[] destinationBytes = new byte[destinationBytesLength];
            buffer.get(destinationBytes);
            String destination = new String(destinationBytes, StandardCharsets.UTF_8);
            int partition = buffer.getInt();
            return new NotificationDestination(destination, partition);
        }

    }

}
