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

import com.dynatrace.research.s3shuffler.OutputPartitioner;
import com.dynatrace.research.s3shuffler.Refresher;
import com.dynatrace.research.s3shuffler.S3BatchFacade;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;

public class S3Shuffler<K,V> {

    private final S3BatchFacade s3BatchFacade;

    private final Serde<K> keySerde;

    private final Serde<V> valueSerde;

    private final long batchDesiredBytes;

    private final Duration batchMaxDuration;

    private final String repartitionName;

    private final OutputPartitioner outputPartitioner;

    private final Refresher rackAssignmentRefresher;

    private String applicationId; // TODO Try to avoid storing this

    private List<String> kafkaBootstrapServers; // TODO Try to avoid storing this

    private AdminClient adminClient; // TODO Try to avoid storing this

    private final TaskSharedBatcher taskSharedBatcher = new TaskSharedBatcher(); // TODO Has to be per thread

    public S3Shuffler(
            S3BatchFacade s3BatchFacade,
            Serde<K> keySerde,
            Serde<V> valueSerde,
            long batchDesiredBytes,
            Duration batchMaxDuration,
            String repartitionName,
            Duration kafkaRackAssignmentRefreshInterval) {
        this(s3BatchFacade, keySerde, valueSerde, batchDesiredBytes, batchMaxDuration, repartitionName, null, kafkaRackAssignmentRefreshInterval);
    }

    public S3Shuffler(
            S3BatchFacade s3BatchFacade,
            Serde<K> keySerde,
            Serde<V> valueSerde,
            long batchDesiredBytes,
            Duration batchMaxDuration,
            String repartitionName,
            Map<Integer, String> staticOutputPartitionsToRacks,
            Duration kafkaRackAssignmentRefreshInterval) {
            //Refresher rackAssignmentRefresher,
            //String applicationId, // TODO Try to avoid passing this
            //String bootstrapServers) { // TODO Try to avoid passing this
        this.s3BatchFacade = s3BatchFacade;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.batchDesiredBytes = batchDesiredBytes;
        this.batchMaxDuration = batchMaxDuration;
        this.repartitionName = repartitionName;
        //this.applicationId = applicationId;
        //Properties adminProperties = new Properties();
        //adminProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        //this.adminClient = AdminClient.create(adminProperties);
        // TODO Does it make sense to provide a static config?
        if (staticOutputPartitionsToRacks != null) {
            this.outputPartitioner = new OutputPartitioner(staticOutputPartitionsToRacks);
        } else {
            this.outputPartitioner = new OutputPartitioner(new HashMap<>());
        }
        if (!kafkaRackAssignmentRefreshInterval.equals(Duration.ZERO)) {
            this.rackAssignmentRefresher = Refresher.create(kafkaRackAssignmentRefreshInterval, this::refreshPartitionRackAssignment);
        } else {
            this.rackAssignmentRefresher = Refresher.NOOP;
        }
    }

    public ProcessorSupplier<K, V, NotificationDestination, NotificationDetails> batcherSupplier() {
        return new S3BatcherProcessor.Supplier<>(
                s3BatchFacade,
                outputPartitioner,
                keySerde,
                valueSerde,
                batchDesiredBytes,
                batchMaxDuration,
                taskSharedBatcher,
                this::handleProcessorInit);
    }

    public ProcessorSupplier<NotificationDestination, NotificationDetails, K, V> debatcherSupplier() {
        return new S3DebatcherProcessor.Supplier<>(s3BatchFacade, keySerde, valueSerde);
    }

    public Repartitioned<NotificationDestination, NotificationDetails> repartitioned() {
        return Repartitioned
                .with(new NotificationDestinationSerde(), new NotificationDetailsSerde())
                .withName(repartitionName)
                .withStreamPartitioner(new NotificationStreamPartitioner());
    }

    public Produced<NotificationDestination, NotificationDetails> produced() {
        return Produced
                .with(new NotificationDestinationSerde(), new NotificationDetailsSerde())
                //.withName(repartitionName)
                .withStreamPartitioner(new NotificationStreamPartitioner());
    }

    public Consumed<NotificationDestination, NotificationDetails> consumed() {
        return Consumed
                .with(new NotificationDestinationSerde(), new NotificationDetailsSerde());
                //.withName(repartitionName)

    }

    private void handleProcessorInit(ProcessorContext<?, ?> context) {
        System.out.println("Refresh partition assignment...");
        this.initRefresherOrFail(
                (List<String>) context.appConfigs().get(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG),
                context.applicationId());
        this.rackAssignmentRefresher.runOnce();
    }

    private void initRefresherOrFail(List<String> kafkaBootstrapServers, String applicationId) {
        if (this.applicationId != null && !this.applicationId.equals(applicationId)) {
            throw new RuntimeException("Different processors provided different applicationIds");
        }
        this.applicationId = applicationId;
        if (this.adminClient != null && !this.kafkaBootstrapServers.equals(kafkaBootstrapServers)) {
            throw new RuntimeException("Different processors provided different bootstrap servers");
        }
        this.kafkaBootstrapServers = kafkaBootstrapServers;
        Properties adminProperties = new Properties();
        adminProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        this.adminClient = AdminClient.create(adminProperties);
    }

    private void refreshPartitionRackAssignment() {
        // TODO can this be derived automatically?
        String effectiveNotificationTopicName = this.applicationId + "-" + this.repartitionName + "-repartition";
        Map<String, TopicDescription> topicDescriptions;
        try {
            topicDescriptions = this.adminClient
                    .describeTopics(List.of(effectiveNotificationTopicName))
                    .allTopicNames()
                    .get();
        } catch (ExecutionException e) {
            topicDescriptions = Map.of();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // Update notification topic assignment
    /*
    Map<String,List<Integer>> mapBuilder = new HashMap<>();
    for (TopicPartitionInfo partitionInfo : topicDescriptions.get(this.kafkaTopicNotifications).partitions()) {
      int partition = partitionInfo.partition();
      String rack = partitionInfo.leader().rack(); // Might be null
      mapBuilder.computeIfAbsent(rack, x -> new ArrayList<>()).add(partition);
    }
    this.racksToNotificationPartitions = mapBuilder.entrySet()
            .stream()
            .peek(e -> Collections.shuffle(e.getValue()))
            .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    e -> new CircularPartitions(e.getValue())));
     */

        // Update notification topic assignment
        Map<Integer,String> outputPartitionsToRack = new HashMap<>();
        if (topicDescriptions.containsKey(effectiveNotificationTopicName)) {
            for (TopicPartitionInfo partitionInfo : topicDescriptions.get(effectiveNotificationTopicName).partitions()) {
                int partition = partitionInfo.partition();
                String rack = partitionInfo.leader().rack(); // Might be null
                outputPartitionsToRack.put(partition, rack);
            }
        }
        //this.outputPartitioner = new OutputPartitioner(outputPartitionsToRack);
        this.outputPartitioner.setOutputPartitionsToRack(outputPartitionsToRack);
    }


}
