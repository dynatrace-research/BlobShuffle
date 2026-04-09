package com.dynatrace.research.blobshuffle.standalone;

import com.dynatrace.research.blobshuffle.*;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.apache5.Apache5HttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

@Testcontainers
class StandaloneBlobShuffleE2ETest {

    private static final Logger LOGGER = LoggerFactory.getLogger(StandaloneBlobShuffleE2ETest.class);

    @Container
    private LocalStackContainer localstack = new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.5.0"))
            .withServices(S3);

    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    void testProduceAndConsumeWith3NodeCluster() throws Exception {
        int numRecords = 200;

        try (KafkaContainerCluster cluster = new KafkaContainerCluster(3, 3)) {
            cluster.start();
            assertThat(cluster.getBrokers()).hasSize(3);

            S3Client s3Client = createS3Client();
            String bucketName = "test-bucket";
            s3Client.createBucket(request -> request.bucket(bucketName));
            assertThat(s3Client.listBuckets().buckets())
                    .extracting("name")
                    .contains(bucketName);

            String bootstrapServers = cluster.getBootstrapServers();
            String notificationTopic = "notifications";

            createKafkaTopic(bootstrapServers, notificationTopic);

            S3Config s3Config = new S3Config(s3Client, bucketName);
            S3BatchFacade s3BatchFacade = s3Config.toS3BatchFacade(new SimpleMeterRegistry());

            // Build a static partition-to-rack mapping for the 10 notification partitions
            Map<Integer, String> partitionsToRacks = new HashMap<>();
            for (int i = 0; i < 10; i++) {
                partitionsToRacks.put(i, "all");
            }
            OutputPartitioner outputPartitioner = new OutputPartitioner(Partitioner.KAFKA_BUILTIN, partitionsToRacks);

            // --- Phase 1: Produce records ---
            List<List<Byte>> producedKeys = new ArrayList<>();
            try (BlobShuffleProducer<byte[], byte[]> producer = new BlobShuffleProducer<>(
                    s3BatchFacade,
                    outputPartitioner,
                    new ByteArraySerializer(),
                    new ByteArraySerializer(),
                    1024 * 1024, // 1 MB batch size
                    Duration.ofSeconds(2),
                    notificationTopic,
                    bootstrapServers,
                    new SimpleMeterRegistry())) {
                Random random = new Random(42);
                LOGGER.info("Producing {} records...", numRecords);
                for (int i = 0; i < numRecords; i++) {
                    byte[] key = new byte[4];
                    random.nextBytes(key);
                    byte[] value = new byte[1024];
                    random.nextBytes(value);
                    producedKeys.add(toByteList(key));
                    producer.send(new ProducerRecord<>(notificationTopic, key, value));
                }
                producer.flush();
                LOGGER.info("All {} records produced and flushed.", numRecords);
            }

            // --- Phase 2: Consume records ---
            List<List<Byte>> consumedKeys = new ArrayList<>();
            Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            try (BlobShuffleConsumer<byte[], byte[]> consumer = new BlobShuffleConsumer<>(
                    s3BatchFacade,
                    new ByteArrayDeserializer(),
                    new ByteArrayDeserializer(),
                    bootstrapServers,
                    "blobshuffle-standalone-e2e",
                    consumerProps,
                    new SimpleMeterRegistry())) {
                consumer.subscribe(Collections.singletonList(notificationTopic));

                LOGGER.info("Consuming records...");
                int consecutiveEmptyPolls = 0;
                while (consecutiveEmptyPolls < 20) {
                    List<ConsumerRecord<byte[], byte[]>> records = consumer.poll(Duration.ofMillis(500));
                    if (records.isEmpty()) {
                        consecutiveEmptyPolls++;
                        continue;
                    }
                    consecutiveEmptyPolls = 0;
                    for (ConsumerRecord<byte[], byte[]> record : records) {
                        consumedKeys.add(toByteList(record.key()));
                    }
                }
                consumer.commitSync();
            }
            LOGGER.info("Consumed {} records total.", consumedKeys.size());

            // --- Phase 3: Verify ---
            Collections.sort(producedKeys, StandaloneBlobShuffleE2ETest::compareByteList);
            Collections.sort(consumedKeys, StandaloneBlobShuffleE2ETest::compareByteList);
            assertThat(consumedKeys).hasSize(producedKeys.size());
            assertThat(consumedKeys).isEqualTo(producedKeys);

            s3BatchFacade.close();
            s3Client.close();
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    void testSimultaneousProduceAndConsume() throws Exception {
        int produceDurationSeconds = 30;

        try (KafkaContainerCluster cluster = new KafkaContainerCluster(3, 3)) {
            cluster.start();
            assertThat(cluster.getBrokers()).hasSize(3);

            S3Client s3Client = createS3Client();
            String bucketName = "test-bucket-simultaneous";
            s3Client.createBucket(request -> request.bucket(bucketName));

            String bootstrapServers = cluster.getBootstrapServers();
            String notificationTopic = "notifications-simultaneous";
            createKafkaTopic(bootstrapServers, notificationTopic);

            S3BatchFacade s3BatchFacade = new S3Config(s3Client, bucketName)
                    .toS3BatchFacade(new SimpleMeterRegistry());

            Map<Integer, String> partitionsToRacks = new HashMap<>();
            for (int i = 0; i < 10; i++) {
                partitionsToRacks.put(i, "all");
            }
            OutputPartitioner outputPartitioner = new OutputPartitioner(Partitioner.KAFKA_BUILTIN, partitionsToRacks);

            List<List<Byte>> producedKeys = Collections.synchronizedList(new ArrayList<>());
            List<List<Byte>> consumedKeys = Collections.synchronizedList(new ArrayList<>());
            AtomicBoolean producerDone = new AtomicBoolean(false);

            // Consumer runs in a background thread, draining until the producer is done
            // and no more notifications arrive.
            Thread consumerThread = Thread.ofVirtual().start(() -> {
                Properties consumerProps = new Properties();
                consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
                try (BlobShuffleConsumer<byte[], byte[]> consumer = new BlobShuffleConsumer<>(
                        s3BatchFacade,
                        new ByteArrayDeserializer(),
                        new ByteArrayDeserializer(),
                        bootstrapServers,
                        "blobshuffle-simultaneous-e2e",
                        consumerProps,
                        new SimpleMeterRegistry())) {
                    consumer.subscribe(Collections.singletonList(notificationTopic));
                    int consecutiveEmptyPolls = 0;
                    while (!producerDone.get() || consecutiveEmptyPolls < 20) {
                        List<ConsumerRecord<byte[], byte[]>> records = consumer.poll(Duration.ofMillis(500));
                        if (records.isEmpty()) {
                            if (producerDone.get()) consecutiveEmptyPolls++;
                            continue;
                        }
                        consecutiveEmptyPolls = 0;
                        for (ConsumerRecord<byte[], byte[]> record : records) {
                            consumedKeys.add(toByteList(record.key()));
                        }
                        consumer.commitSync();
                    }
                }
            });

            // Producer runs on the test thread for produceDurationSeconds.
            Random random = new Random(42);
            long deadline = System.currentTimeMillis() + produceDurationSeconds * 1000L;
            try (BlobShuffleProducer<byte[], byte[]> producer = new BlobShuffleProducer<>(
                    s3BatchFacade,
                    outputPartitioner,
                    new ByteArraySerializer(),
                    new ByteArraySerializer(),
                    1024 * 1024,
                    Duration.ofSeconds(2),
                    notificationTopic,
                    bootstrapServers,
                    new SimpleMeterRegistry())) {
                while (System.currentTimeMillis() < deadline) {
                    byte[] key = new byte[4];
                    random.nextBytes(key);
                    byte[] value = new byte[1024];
                    random.nextBytes(value);
                    producedKeys.add(toByteList(key));
                    producer.send(new ProducerRecord<>(notificationTopic, key, value));
                }
                LOGGER.info("Produced {} records. Flushing...", producedKeys.size());
                producer.flush();
            }
            producerDone.set(true);
            LOGGER.info("Producer done. Waiting for consumer to drain.");

            consumerThread.join();
            LOGGER.info("Consumer done. Consumed {} records.", consumedKeys.size());

            Collections.sort(producedKeys, StandaloneBlobShuffleE2ETest::compareByteList);
            Collections.sort(consumedKeys, StandaloneBlobShuffleE2ETest::compareByteList);
            assertThat(consumedKeys).hasSize(producedKeys.size());
            assertThat(consumedKeys).isEqualTo(producedKeys);

            s3BatchFacade.close();
            s3Client.close();
        }
    }

    private S3Client createS3Client() {
        return S3Client.builder()
                .httpClient(Apache5HttpClient.builder().maxConnections(50).build())
                .endpointOverride(localstack.getEndpoint())
                .credentialsProvider(
                        StaticCredentialsProvider.create(
                                AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey())
                        )
                )
                .region(Region.of(localstack.getRegion()))
                .build();
    }

    private void createKafkaTopic(String bootstrapServers, String name) {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.CLIENT_ID_CONFIG, "e2e-test-topic-create-client");
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        try (AdminClient adminClient = AdminClient.create(properties)) {
            adminClient.createTopics(List.of(
                    new NewTopic(name, 10, (short) 1))).all().get();
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<Byte> toByteList(byte[] bytes) {
        List<Byte> list = new ArrayList<>(bytes.length);
        for (byte b : bytes) {
            list.add(b);
        }
        return list;
    }

    private static int compareByteList(List<Byte> a, List<Byte> b) {
        for (int i = 0; i < Math.min(a.size(), b.size()); i++) {
            int cmp = Byte.compare(a.get(i), b.get(i));
            if (cmp != 0) return cmp;
        }
        return Integer.compare(a.size(), b.size());
    }
}
