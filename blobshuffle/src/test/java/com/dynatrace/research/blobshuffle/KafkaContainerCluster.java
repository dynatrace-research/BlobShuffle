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
package com.dynatrace.research.blobshuffle;

import org.apache.kafka.common.Uuid;
import org.awaitility.Awaitility;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;


public class KafkaContainerCluster implements Startable {

    private final int brokersNum;

    private final Network network;

    private final Collection<KafkaContainer> brokers;

    public KafkaContainerCluster(int brokersNum, int internalTopicsRf) {
        if (brokersNum < 0) {
            throw new IllegalArgumentException("brokersNum '" + brokersNum + "' must be greater than 0");
        }
        if (internalTopicsRf < 0 || internalTopicsRf > brokersNum) {
            throw new IllegalArgumentException(
                    "internalTopicsRf '" + internalTopicsRf + "' must be less than brokersNum and greater than 0"
            );
        }

        this.brokersNum = brokersNum;
        this.network = Network.newNetwork();

        String controllerQuorumVoters = IntStream
                .range(0, brokersNum)
                .mapToObj(brokerNum -> String.format("%d@broker-%d:9094", brokerNum, brokerNum))
                .collect(Collectors.joining(","));

        String clusterId = Uuid.randomUuid().toString();

        this.brokers =
                IntStream
                        .range(0, brokersNum)
                        .mapToObj(brokerNum -> {
                            KafkaContainer c = new KafkaContainer(DockerImageName.parse("apache/kafka:3.9.0"))
                                    .withNetwork(this.network)
                                    .withNetworkAliases("broker-" + brokerNum)
                                    .withEnv("CLUSTER_ID", clusterId)
                                    .withEnv("KAFKA_BROKER_ID", brokerNum + "")
                                    .withEnv("KAFKA_NODE_ID", brokerNum + "")
                                    .withEnv("KAFKA_BROKER_RACK", "az" + brokerNum)
                                    .withEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", controllerQuorumVoters)
                                    .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", internalTopicsRf + "")
                                    .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
                                    .withEnv("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", internalTopicsRf + "")
                                    .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", internalTopicsRf + "")
                                    .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", internalTopicsRf + "")
                                    .withEnv("KAFKA_LISTENERS", "PLAINTEXT://:9092,BROKER://:9093,CONTROLLER://:9094") // required for Kafka 3.9
                                    .withStartupTimeout(Duration.ofMinutes(1));
                            return c;
                        })
                        .collect(Collectors.toList());
    }

    public Collection<KafkaContainer> getBrokers() {
        return this.brokers;
    }

    public String getBootstrapServers() {
        return brokers.stream().map(KafkaContainer::getBootstrapServers).collect(Collectors.joining(","));
    }

    @Override
    public void start() {
        // Needs to start all the brokers at once
        brokers.parallelStream().forEach(GenericContainer::start);

        Awaitility
                .await()
                .atMost(Duration.ofSeconds(1800))
                .untilAsserted(() -> {
                    Container.ExecResult result =
                            this.brokers.stream()
                                    .findFirst()
                                    .get()
                                    .execInContainer(
                                            "sh",
                                            "-c",
                                            "/opt/kafka/bin/kafka-log-dirs.sh --bootstrap-server localhost:9093 --describe | grep -o '\"broker\"' | wc -l"
                                    );
                    String brokers = result.getStdout().replace("\n", "");

                    assertThat(brokers).asInt().isEqualTo(this.brokersNum);
                });
    }

    @Override
    public void stop() {
        this.brokers.stream().parallel().forEach(GenericContainer::stop);
    }
}