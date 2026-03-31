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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
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

import static org.assertj.core.api.Assertions.*;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

@Testcontainers
class KafkaStreamsBlobShuffleE2ETest {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaStreamsBlobShuffleE2ETest.class);

    private final Hasher64 hasher = Hashing.komihash4_3(0x81c1f2b68962424dL);

    @Container
    private LocalStackContainer localstack = new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.5.0"))
            .withServices(S3);

    @Test
    void testWith3NodeKafkaCluster() throws Exception {
        try (KafkaContainerCluster cluster = new KafkaContainerCluster(3, 3)) {
            cluster.start();
            assertThat(cluster.getBrokers()).hasSize(3);

            S3Client s3Client = S3Client
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
            String bucketName = "test-bucket";
            s3Client.createBucket(request -> request.bucket(bucketName));
            ListBucketsResponse listBucketsResponse = s3Client.listBuckets(); // Will show max. 1000
            List<Bucket> allBuckets = listBucketsResponse.buckets();
            Assertions.assertTrue(allBuckets.stream().map(Bucket::name).anyMatch(name -> name.equals(bucketName)));

            String bootstrapServers = cluster.getBootstrapServers();
            String zone = "az1";
            String inputTopic = "input";
            String outputTopic = "output";
            String notificationsTopic = "notifications";

            createKafkaTopic(bootstrapServers, inputTopic);
            createKafkaTopic(bootstrapServers, outputTopic);

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
                    Map.of()
            );
            application.start();

            List<Integer> producedRecords = new ArrayList<>(2 * 1000 * 30);
            KafkaLoadGenerator loadGenerator = new KafkaLoadGenerator(
                    "load-generator-0",
                    bootstrapServers,
                    inputTopic,
                    1,
                    true,
                    1000,
                    1024,
                    1,
                    Map.of(),
                    // Ignore last 8 bytes (they will be replaced by the timestamp in the output topic)
                    r -> producedRecords.add(hasher.hashBytesToInt(r.getData(), 0, r.getData().length - 8))
            );
            loadGenerator.startAsync();

            Thread.sleep(30_000);

            LOGGER.info("Shut down load generator.");
            loadGenerator.stop();
            LOGGER.info("Produced {} records.", producedRecords.size());

            // Consume output while the app is still running so the pipeline
            // can fully drain before we initiate shutdown (avoids S3 timeout
            // errors during Kafka Streams flush/commit on close).
            LOGGER.info("Start consuming output records while app is still running.");
            Properties consumerProperties = new Properties();
            consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "blobshuffle-test");
            consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            List<Integer> consumedRecords = new ArrayList<>(2 * 1000 * 30);
            try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(
                    consumerProperties,
                    new ByteArrayDeserializer(),
                    new ByteArrayDeserializer())) {
                consumer.subscribe(List.of(outputTopic));
                long deadlineMs = System.currentTimeMillis() + Duration.ofMinutes(3).toMillis();
                int consecutiveEmptyPolls = 0;
                while (System.currentTimeMillis() < deadlineMs) {
                    ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
                    if (records.isEmpty()) {
                        consecutiveEmptyPolls++;
                        // Only give up on idle if we already have all expected records
                        if (consumedRecords.size() >= producedRecords.size() && consecutiveEmptyPolls >= 10) {
                            LOGGER.info("All {} expected records consumed and no more arriving.", consumedRecords.size());
                            break;
                        }
                        continue;
                    }
                    consecutiveEmptyPolls = 0;
                    for (ConsumerRecord<byte[], byte[]> record : records) {
                        // Ignore last 8 bytes (which contain the timestamp)
                        consumedRecords.add(hasher.hashBytesToInt(record.value(), 0, record.value().length - 8));
                    }
                }
                LOGGER.info("Consumer finished. Consumed {} of {} expected records.",
                        consumedRecords.size(), producedRecords.size());
            }

            LOGGER.info("Shut down Kafka Streams app.");
            application.stop();

            Collections.sort(producedRecords);
            Collections.sort(consumedRecords);
            Assertions.assertEquals(producedRecords.size(), consumedRecords.size());
            Assertions.assertEquals(producedRecords, consumedRecords);
        }
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