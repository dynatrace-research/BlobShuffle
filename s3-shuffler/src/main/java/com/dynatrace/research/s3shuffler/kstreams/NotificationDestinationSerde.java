package com.dynatrace.research.s3shuffler.kstreams;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class NotificationDestinationSerde implements Serde<NotificationDestination> {

    @Override
    public Serializer<NotificationDestination> serializer() {
        return new NotificationDestinationSerializer();
    }

    @Override
    public Deserializer<NotificationDestination> deserializer() {
        return new NotificationDestinationDeserializer();
    }

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
