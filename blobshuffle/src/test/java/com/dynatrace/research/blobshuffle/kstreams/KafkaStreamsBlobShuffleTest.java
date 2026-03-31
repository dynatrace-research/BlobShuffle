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

import com.dynatrace.hash4j.hashing.Hasher64;
import com.dynatrace.hash4j.hashing.Hashing;
import com.dynatrace.research.blobshuffle.*;
import com.dynatrace.research.shufflebench.KafkaLoadGenerator;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.Assertions;
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
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.*;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

@Testcontainers
class KafkaStreamsBlobShuffleE2ETest {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaStreamsBlobShuffleE2ETest.class);

    private final Hasher64 hasher = Hashing.komihash4_3(0x81c1f2b68962424dL);

    @Container
    private LocalStackContainer localstack = new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.5.0"))
            .withServices(S3);

    /**
     * Runs the full BlobShuffle pipeline with a 3-node Kafka cluster,
     * then stops everything and verifies that all produced records
     * appear in the output topic.
     */
    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    void testWith3NodeKafkaCluster() throws Exception {
        // Use low throughput so the pipeline can keep up in CI and
        // all batches are drained before shutdown.
        int recordsPerSecond = 200;
        int loadDurationSeconds = 15;

        try (KafkaContainerCluster cluster = new KafkaContainerCluster(3, 3)) {
            cluster.start();
            assertThat(cluster.getBrokers()).hasSize(3);

            S3Client s3Client = createS3Client();
            String bucketName = "test-bucket";
            s3Client.createBucket(request -> request.bucket(bucketName));
            ListBucketsResponse listBucketsResponse = s3Client.listBuckets();
            List<Bucket> allBuckets = listBucketsResponse.buckets();
            Assertions.assertTrue(allBuckets.stream().map(Bucket::name).anyMatch(name -> name.equals(bucketName)));

            String bootstrapServers = cluster.getBootstrapServers();
            String zone = "az1";
            String inputTopic = "input";
            String outputTopic = "output";
            String notificationsTopic = "notifications";

            createKafkaTopic(bootstrapServers, inputTopic);
            createKafkaTopic(bootstrapServers, outputTopic);

            // Use a long commit interval to avoid triggering flushes while
            // the debatcher still has pending S3 fetches (LocalStack is slow in CI).
            KafkaStreamsBlobShuffleExample application = new KafkaStreamsBlobShuffleExample(
                    KafkaStreamsBlobShuffleExample.Topology.FULL,
                    Optional.of(zone),
                    s3Client,
                    bucketName,
                    LocalCacheConfig.disabled(),
                    DistributedCacheConfig.disabled(),
                    bootstrapServers,
                    inputTopic,
                    notificationsTopic,
                    outputTopic,
                    Duration.ofSeconds(30),
                    1024 * 1024,
                    Duration.ofSeconds(5),
                    Partitioner.KAFKA_BUILTIN,
                    Map.of(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "60000")
            );
            application.start();

            // --- Phase 1: Produce records ---
            List<Integer> producedRecords = new ArrayList<>(2 * recordsPerSecond * loadDurationSeconds);
            KafkaLoadGenerator loadGenerator = new KafkaLoadGenerator(
                    "load-generator-0",
                    bootstrapServers,
                    inputTopic,
                    1,
                    true,
                    recordsPerSecond,
                    1024,
                    1,
                    Map.of(),
                    // Ignore last 8 bytes (they will be replaced by the timestamp in the output topic)
                    r -> producedRecords.add(hasher.hashBytesToInt(r.getData(), 0, r.getData().length - 8))
            );
            loadGenerator.startAsync();

            Thread.sleep(loadDurationSeconds * 1000L);

            LOGGER.info("Shut down load generator.");
            loadGenerator.stop();
            LOGGER.info("Produced {} records.", producedRecords.size());

            // --- Phase 2: Wait for pipeline to drain, then stop ---
            // batchMaxDuration is 5s, so the last batch is flushed within ~5s.
            // Then S3 write + notification + repartition + S3 read + output.
            // Give plenty of time for this to complete in CI.
            LOGGER.info("Wait 30 seconds for the pipeline to drain.");
            Thread.sleep(30_000);

            LOGGER.info("Shut down Kafka Streams app.");
            application.stop();

            // --- Phase 3: Consume output and verify ---
            LOGGER.info("Start consuming output records.");
            List<Integer> consumedRecords = consumeAllRecords(bootstrapServers, outputTopic);
            LOGGER.info("Consumed {} records.", consumedRecords.size());

            Collections.sort(producedRecords);
            Collections.sort(consumedRecords);
            Assertions.assertEquals(producedRecords.size(), consumedRecords.size());
            Assertions.assertEquals(producedRecords, consumedRecords);
        }
    }

    private S3Client createS3Client() {
        return S3Client
                .builder()
                .httpClient(Apache5HttpClient.builder().maxConnections(100).build())
                .endpointOverride(localstack.getEndpoint())
                .credentialsProvider(
                        StaticCredentialsProvider.create(
                                AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey())
                        )
                )
                .region(Region.of(localstack.getRegion()))
                .build();
    }

    /**
     * Consume all records from a topic, tolerating brief gaps in delivery.
     * Returns when 20 consecutive polls (10s) return no records.
     */
    private List<Integer> consumeAllRecords(String bootstrapServers, String topic) {
        Properties consumerProperties = new Properties();
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "blobshuffle-test");
        consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        List<Integer> records = new ArrayList<>();
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(
                consumerProperties,
                new ByteArrayDeserializer(),
                new ByteArrayDeserializer())) {
            consumer.subscribe(List.of(topic));
            int consecutiveEmptyPolls = 0;
            while (consecutiveEmptyPolls < 20) {
                ConsumerRecords<byte[], byte[]> batch = consumer.poll(Duration.ofMillis(500));
                if (batch.isEmpty()) {
                    consecutiveEmptyPolls++;
                    continue;
                }
                consecutiveEmptyPolls = 0;
                for (ConsumerRecord<byte[], byte[]> record : batch) {
                    // Ignore last 8 bytes (which contain the timestamp)
                    records.add(hasher.hashBytesToInt(record.value(), 0, record.value().length - 8));
                }
            }
        }
        return records;
    }

    private void createKafkaTopic(String bootstrapServers, String name) {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        try (AdminClient adminClient = AdminClient.create(properties)) {
            adminClient.createTopics(List.of(
                    new NewTopic(name, 10, (short) 1)));
        }
    }

}