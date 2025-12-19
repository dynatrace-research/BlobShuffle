package com.dynatrace.research.s3shuffler.kstreams;

import com.dynatrace.research.s3shuffler.BasicS3BatchFacade;
import com.dynatrace.research.s3shuffler.CacheConfig;
import com.dynatrace.research.s3shuffler.Partitioner;
import com.dynatrace.research.s3shuffler.S3BatchFacade;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.smallrye.config.SmallRyeConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.assignment.assignors.StickyTaskAssignor;
import org.apache.kafka.streams.processor.internals.assignment.HighAvailabilityTaskAssignor;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.http.apache5.Apache5HttpClient;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
          CacheConfig cacheConfig,
          String kafkaBootstrapServers,
          String kafkaTopicInput,
          String kafkaTopicNotifications,
          String kafkaTopicOutput,
          Duration kafkaRackAssignmentRefreshInterval,
          long batchDesiredBytes,
          Duration batchMaxDuration,
          Partitioner partitioner
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

    final MeterRegistry meterRegistry = getRegistry();

    if (cacheConfig.cacheDistributedEnabled()) {
      LOGGER.info("Use distributed cache of size {} bytes.", cacheConfig.cacheDistributedSizeBytes());
    } else {
      LOGGER.info("Do not use distributed cache.");
    }
    if (cacheConfig.cacheLocalEnabled()) {
      LOGGER.info("Use local cache of size {} bytes.", cacheConfig.cacheLocalSizeBytes());
    } else {
      LOGGER.info("Do not use local cache.");
    }
    S3Shuffler<byte[], byte[]> s3Shuffler = new S3Shuffler<>(
            cacheConfig.toS3BatchFacade(
                    zone.orElse("all"),
                    s3Client,
                    bucketName,
                    batchDesiredBytes,
                    meterRegistry),
            Serdes.ByteArray(),
            Serdes.ByteArray(),
            batchDesiredBytes,
            batchMaxDuration,
            kafkaTopicNotifications,
            partitioner,
            kafkaRackAssignmentRefreshInterval,
            meterRegistry);

    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
    if (zone.isPresent()) {
      props.put(ConsumerConfig.CLIENT_RACK_CONFIG, zone.get());
      props.put(StreamsConfig.CLIENT_TAG_PREFIX + "zone", zone.get());
      props.put(StreamsConfig.RACK_AWARE_ASSIGNMENT_TAGS_CONFIG, "zone");
      props.put(StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_CONFIG, StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC);
    }
    final org.apache.kafka.streams.Topology steamsTopology = buildTopology(topology, s3Shuffler, meterRegistry);
    this.kafkaStreamsApp = new KafkaStreams(steamsTopology, props);
  }

  private org.apache.kafka.streams.Topology buildTopology(Topology topology, S3Shuffler<byte[], byte[]> s3Shuffler, MeterRegistry meterRegistry) {
    final StreamsBuilder builder = new StreamsBuilder();
    LOGGER.info("Build example application with topology {}", topology);
    switch (topology) {
        case FULL -> {

          Timer latencyTimer = Timer.builder("s3shuffler.latency")
                  .description("time between consuming and producing a record") // optional
                  //.tags("region", "test") // optional
                  .publishPercentiles(0.5, 0.95, 0.99, 0.999) // median and 95th, 99th and 99.9th percentile
                  .publishPercentileHistogram()
                  //.serviceLevelObjectives(Duration.ofMillis(100))
                  .minimumExpectedValue(Duration.ofMillis(1))
                  .maximumExpectedValue(Duration.ofSeconds(60))
                  .register(meterRegistry);

                builder.stream(kafkaTopicInput, Consumed.with(Serdes.ByteArray(), Serdes.ByteArray()))
                .selectKey((k,value) -> Arrays.copyOfRange(value, 0, 4))
                .mapValues(value -> {
                  long currentTime = System.currentTimeMillis();
                  ByteBuffer.wrap(value, value.length - 8, 8).putLong(currentTime);
                  return value;
                })
                .process(s3Shuffler.batcherSupplier())
                //.foreach((dest, details) -> new String(dest + "->" + details));
                .repartition(s3Shuffler.repartitioned())
                //.peek((destination, details) -> LOGGER.info("########## Repartitioned to {} -> {}", destination, details))
                .process(s3Shuffler.debatcherSupplier())
                .mapValues(value -> {
                  long currentTime = System.currentTimeMillis();
                  long startTime = ByteBuffer.wrap(value, value.length - 8, 8).getLong();
                  long innerLatency = currentTime - startTime;
                  latencyTimer.record(innerLatency, TimeUnit.MILLISECONDS);
                  return value;
                })
                .to(kafkaTopicOutput, Produced.with(Serdes.ByteArray(), Serdes.ByteArray()));
        }
        case BATCHER -> {
                builder.stream(kafkaTopicInput, Consumed.with(Serdes.ByteArray(), Serdes.ByteArray()))
                        .selectKey((k,value) -> Arrays.copyOfRange(value, 0, 4))
                        .mapValues(value -> {
                          long currentTime = System.currentTimeMillis();
                          ByteBuffer.wrap(value, value.length - 8, 8).putLong(currentTime);
                          return value;
                        })
                        .process(s3Shuffler.batcherSupplier())
                        //.repartition(s3Shuffler.repartitioned())
                        //.foreach((dest, details) -> new String(dest + "->" + details));
                        .to(kafkaTopicNotifications, s3Shuffler.produced());
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

      try (ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor()) {
          executor.scheduleAtFixedRate(() -> {
              kafkaStreamsApp.metadataForLocalThreads().forEach(metadata -> {
                  metadata.activeTasks().forEach(t -> {
                      String topicPartitions = t.topicPartitions().stream().map(tp -> tp.topic() + ":" + tp.partition()).collect(Collectors.joining(","));
                      LOGGER.info("This instance consumes Kafka topic partitions: {}", topicPartitions); // TODO DEBUG
                  });
              });
          }, 10, 10, TimeUnit.SECONDS);
      }

  }

  public void stop() {
    this.kafkaStreamsApp.close();
  }

  private MeterRegistry getRegistry() {
    final PrometheusMeterRegistry prometheusRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    try {
      final HttpServer server = HttpServer.create(new InetSocketAddress(8080), 0);
      server.createContext("/metrics", httpExchange -> {
        String response = prometheusRegistry.scrape();
        httpExchange.sendResponseHeaders(200, response.getBytes().length);
        try (OutputStream os = httpExchange.getResponseBody()) {
          os.write(response.getBytes());
        }
      });
      new Thread(server::start).start();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return prometheusRegistry;
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

    Partitioner partitioner = Partitioner.of(config.getValue("partitioner", String.class));

    boolean cacheLocalEnabled = config.getValue("cache.local.enabled", Boolean.class);
    long cacheLocalSizeBytes = config.getValue("cache.local.size.bytes", Long.class);
    boolean cacheDistributedEnabled = config.getValue("cache.distributed.enabled", Boolean.class);
    long cacheDistributedSizeBytes = config.getValue("cache.distributed.size.bytes", Long.class);
    String cacheDistributedAdvertisedHost = config.getValue("cache.distributed.advertised.host", String.class);
    int cacheDistributedPort = config.getValue("cache.distributed.port", Integer.class);
    CacheConfig cacheConfig = new CacheConfig(cacheLocalEnabled, cacheLocalSizeBytes, cacheDistributedEnabled, cacheDistributedSizeBytes, cacheDistributedAdvertisedHost, cacheDistributedPort);

    KafkaStreamsS3ShufflerExample kafkaStreamsS3Shuffler = new KafkaStreamsS3ShufflerExample(
            topology,
            zone,
            s3Client,
            bucketName,
            cacheConfig,
            kafkaBootstrapServers,
            inputTopicName,
            notificationTopicName,
            outputTopicName,
            Duration.ofSeconds(300),
            batchDesiredBytes,
            batchMaxDuration,
            partitioner
    );
    kafkaStreamsS3Shuffler.start();
  }

}
