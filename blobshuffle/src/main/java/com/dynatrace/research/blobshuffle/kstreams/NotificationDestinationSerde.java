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
package com.dynatrace.research.blobshuffle.kstreams;

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
