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
package com.dynatrace.research.s3shuffler.kstreams;

import com.dynatrace.research.s3shuffler.BasicS3BatchFacade;
import io.smallrye.config.SmallRyeConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.http.apache5.Apache5HttpClient;
import software.amazon.awssdk.services.s3.S3Client;

import java.time.Duration;
import java.util.*;

public class KafkaStreamsS3ShufflerExample {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaStreamsS3ShufflerExample.class);

  private static final String APPLICATION_ID = "s3shuffler-kstreams";

  private final String kafkaTopicInput;

  private final String kafkaTopicNotifications;

  private final String kafkaTopicOutput;

  private final KafkaStreams kafkaStreamsApp;

  public KafkaStreamsS3ShufflerExample(
          Topology topology,
          Optional<String> zone,
          S3Client s3Client,
          String bucketName,
          String kafkaBootstrapServers,
          String kafkaTopicInput,
          String kafkaTopicNotifications,
          String kafkaTopicOutput,
          Duration kafkaRackAssignmentRefreshInterval,
          long batchDesiredBytes,
          Duration batchMaxDuration
  ) {
    //this.zone = zone;
    if (zone.isPresent()) {
      LOGGER.info("Start S3 Batcher with configured zone '{}'.", zone.get());
    } else {
      LOGGER.info("Start S3 Batcher without configured zone.");
    }

    this.kafkaTopicInput = kafkaTopicInput;
    this.kafkaTopicNotifications = kafkaTopicNotifications;
    this.kafkaTopicOutput = kafkaTopicOutput;

    S3Shuffler<byte[], byte[]> s3Shuffler = new S3Shuffler<>(
            new BasicS3BatchFacade(s3Client, bucketName).withCache(10, true),
            Serdes.ByteArray(),
            Serdes.ByteArray(),
            batchDesiredBytes,
            batchMaxDuration,
            kafkaTopicNotifications,
            kafkaRackAssignmentRefreshInterval);

    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
    if (zone.isPresent()) {
      props.put(ConsumerConfig.CLIENT_RACK_CONFIG, zone.get());
      props.put(StreamsConfig.CLIENT_TAG_PREFIX + "zone", zone.get());
      props.put(StreamsConfig.RACK_AWARE_ASSIGNMENT_TAGS_CONFIG, "zone");
      props.put(StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_CONFIG, StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC);
    }
    final org.apache.kafka.streams.Topology steamsTopology = buildTopology(topology, s3Shuffler);
    this.kafkaStreamsApp = new KafkaStreams(steamsTopology, props);
  }

  private org.apache.kafka.streams.Topology buildTopology(Topology topology, S3Shuffler<byte[], byte[]> s3Shuffler) {
    final StreamsBuilder builder = new StreamsBuilder();
    LOGGER.info("Build example application with topology {}", topology);
    switch (topology) {
        case FULL -> {
                builder.stream(kafkaTopicInput, Consumed.with(Serdes.ByteArray(), Serdes.ByteArray()))
                .selectKey((k,value) -> Arrays.copyOfRange(value, 0, 4))
                .process(s3Shuffler.batcherSupplier())
                //.foreach((dest, details) -> new String(dest + "->" + details));
                .repartition(s3Shuffler.repartitioned())
                .process(s3Shuffler.debatcherSupplier())
                .to(kafkaTopicOutput, Produced.with(Serdes.ByteArray(), Serdes.ByteArray()));
        }
        case BATCHER -> {
                builder.stream(kafkaTopicInput, Consumed.with(Serdes.ByteArray(), Serdes.ByteArray()))
                        .selectKey((k,value) -> Arrays.copyOfRange(value, 0, 4))
                        .process(s3Shuffler.batcherSupplier())
                        .repartition(s3Shuffler.repartitioned())
                        .foreach((dest, details) -> new String(dest + "->" + details));
                        //.to(kafkaTopicNotifications, s3Shuffler.produced());
        }
        case DEBATCHER -> {
                builder.stream(kafkaTopicNotifications, s3Shuffler.consumed())
                        .process(s3Shuffler.debatcherSupplier())
                        .to(kafkaTopicOutput, Produced.with(Serdes.ByteArray(), Serdes.ByteArray()));
        }
      }
      return builder.build();
  }

  public void start() {
    this.kafkaStreamsApp.start();
  }

  public void stop() {
    this.kafkaStreamsApp.close();
  }

  public enum Topology {
    FULL("Full"), BATCHER("Batcher"), DEBATCHER("Debatcher");

    private final String name;

    Topology(String name) {
      this.name = name;
    }

    public static Topology forName(String name) {
      return Arrays.stream(Topology.values())
              .filter(t -> t.name.equals(name))
              .findFirst()
              .orElseThrow(() -> new IllegalArgumentException("Topology for name " + name + " does not exist"));
    }
  }


  public static void main(String[] args) {
    final SmallRyeConfig config = ConfigProvider.getConfig().unwrap(SmallRyeConfig.class);

    Topology topology = Topology.forName(config.getValue("topology", String.class));

    Optional<String> zone = config.getOptionalValue("zone", String.class);

    //final String accessKey = "<AWS Access Key>";
    //final String secretKey = "<AWS Secret Key>";
    //AwsCredentialsProvider credentialProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey));
    AwsCredentialsProvider credentialProvider = DefaultCredentialsProvider.create();
    final String bucketName = config.getValue("s3.bucket", String.class);
    //final String regionName = "<AWS Region>";
    S3Client s3Client = S3Client
            .builder()
            .httpClient(Apache5HttpClient.builder().maxConnections(100).build())
            //.region(Region.of(regionName))
            .credentialsProvider(credentialProvider)
            .build();

    String kafkaBootstrapServers = config.getValue("kafka.bootstrap.servers", String.class);
    LOGGER.info("Use Kafka bootstrap servers {}.", kafkaBootstrapServers);
    String inputTopicName = config.getValue("kafka.topic.input", String.class);
    LOGGER.info("Use input topic {}.", inputTopicName);
    String notificationTopicName = config.getValue("kafka.topic.notifications", String.class);
    LOGGER.info("Use notifications topic {}.", notificationTopicName);
    String outputTopicName = config.getValue("kafka.topic.output", String.class);
    LOGGER.info("Use output topic {}.", outputTopicName);

    long batchDesiredBytes = config.getValue("batch.desired.bytes", Long.class);
    Duration batchMaxDuration = Duration.ofMillis(config.getValue("batch.max.ms", Long.class));

    KafkaStreamsS3ShufflerExample kafkaStreamsS3Shuffler = new KafkaStreamsS3ShufflerExample(
            topology,
            zone,
            s3Client,
            bucketName,
            kafkaBootstrapServers,
            inputTopicName,
            notificationTopicName,
            outputTopicName,
            Duration.ofSeconds(30),
            batchDesiredBytes,
            batchMaxDuration
    );
    kafkaStreamsS3Shuffler.start();
  }

}
