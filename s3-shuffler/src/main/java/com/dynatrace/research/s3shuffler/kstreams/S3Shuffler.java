package com.dynatrace.research.s3shuffler.kstreams;

import com.dynatrace.research.s3shuffler.*;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
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

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;

/**
 * Main entry point for integrating BlobShuffle into a Kafka Streams application.
 * <p>
 * Provides {@link ProcessorSupplier} factories for the Batcher and Debatcher operators
 * and the repartitioning configuration that replaces a standard Kafka Streams
 * {@code .repartition()} call.
 * <p>
 * Usage (abbreviated):
 * <pre>
 * S3Shuffler&lt;MyKey, MyValue&gt; shuffler = new S3Shuffler&lt;&gt;(...);
 * builder.stream("input")
 *        .process(shuffler.batcherSupplier())
 *        .repartition(shuffler.repartitioned())
 *        .process(shuffler.debatcherSupplier())
 *        ...
 * shuffler.close();
 * </pre>
 *
 * @param <K> the record key type
 * @param <V> the record value type
 */
public class S3Shuffler<K,V> implements Closeable {

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

    private final MeterRegistry meterRegistry;

    private final TaskSharedBatcher taskSharedBatcher = new TaskSharedBatcher(); // TODO Has to be per thread

    /**
     * Creates a new S3Shuffler with the default partitioner ({@link Partitioner#KAFKA_BUILTIN})
     * and a default {@link SimpleMeterRegistry}.
     *
     * @param s3Config S3 client and bucket configuration
     * @param localCacheConfig local (per-instance) cache configuration
     * @param distributedCacheConfig distributed (per-AZ) cache configuration
     * @param zone the availability zone identifier of this instance
     * @param keySerde the key serde
     * @param valueSerde the value serde
     * @param batchDesiredBytes the target batch size in bytes
     * @param batchMaxDuration the maximum time before a batch is finalized
     * @param repartitionName name for the internal repartitioning topic
     * @param kafkaRackAssignmentRefreshInterval interval for refreshing the partition-to-rack
     *                                           mapping; use {@link Duration#ZERO} to disable
     */
    public S3Shuffler(
            S3Config s3Config,
            LocalCacheConfig localCacheConfig,
            DistributedCacheConfig distributedCacheConfig,
            String zone,
            Serde<K> keySerde,
            Serde<V> valueSerde,
            long batchDesiredBytes,
            Duration batchMaxDuration,
            String repartitionName,
            Duration kafkaRackAssignmentRefreshInterval) {
        this(
                s3Config,
                localCacheConfig,
                distributedCacheConfig,
                zone,
                keySerde,
                valueSerde,
                batchDesiredBytes,
                batchMaxDuration,
                repartitionName,
                Partitioner.KAFKA_BUILTIN,
                kafkaRackAssignmentRefreshInterval,
                new SimpleMeterRegistry());
    }

    /**
     * Creates a new S3Shuffler with a custom partitioner and meter registry.
     *
     * @param s3Config S3 client and bucket configuration
     * @param localCacheConfig local (per-instance) cache configuration
     * @param distributedCacheConfig distributed (per-AZ) cache configuration
     * @param zone the availability zone identifier of this instance
     * @param keySerde the key serde
     * @param valueSerde the value serde
     * @param batchDesiredBytes the target batch size in bytes
     * @param batchMaxDuration the maximum time before a batch is finalized
     * @param repartitionName name for the internal repartitioning topic
     * @param partitioner the partitioning strategy
     * @param kafkaRackAssignmentRefreshInterval interval for refreshing the partition-to-rack mapping
     * @param meterRegistry the meter registry for metrics
     */
    public S3Shuffler(
            S3Config s3Config,
            LocalCacheConfig localCacheConfig,
            DistributedCacheConfig distributedCacheConfig,
            String zone,
            Serde<K> keySerde,
            Serde<V> valueSerde,
            long batchDesiredBytes,
            Duration batchMaxDuration,
            String repartitionName,
            Partitioner partitioner,
            Duration kafkaRackAssignmentRefreshInterval,
            MeterRegistry meterRegistry) {
        this(
                s3Config,
                localCacheConfig,
                distributedCacheConfig,
                zone,
                keySerde,
                valueSerde,
                batchDesiredBytes,
                batchMaxDuration,
                repartitionName,
                partitioner,
                new HashMap<>(),
                kafkaRackAssignmentRefreshInterval,
                meterRegistry);
    }

    /**
     * Creates a new S3Shuffler with a custom partitioner and static rack assignment,
     * using a default {@link SimpleMeterRegistry}.
     *
     * @param s3Config S3 client and bucket configuration
     * @param localCacheConfig local (per-instance) cache configuration
     * @param distributedCacheConfig distributed (per-AZ) cache configuration
     * @param zone the availability zone identifier of this instance
     * @param keySerde the key serde
     * @param valueSerde the value serde
     * @param batchDesiredBytes the target batch size in bytes
     * @param batchMaxDuration the maximum time before a batch is finalized
     * @param repartitionName name for the internal repartitioning topic
     * @param partitioner the partitioning strategy
     * @param staticOutputPartitionsToRacks static mapping of output partition numbers to rack IDs
     * @param kafkaRackAssignmentRefreshInterval interval for refreshing the partition-to-rack mapping
     */
    public S3Shuffler(
            S3Config s3Config,
            LocalCacheConfig localCacheConfig,
            DistributedCacheConfig distributedCacheConfig,
            String zone,
            Serde<K> keySerde,
            Serde<V> valueSerde,
            long batchDesiredBytes,
            Duration batchMaxDuration,
            String repartitionName,
            Partitioner partitioner,
            Map<Integer, String> staticOutputPartitionsToRacks,
            Duration kafkaRackAssignmentRefreshInterval) {
        this(
                s3Config,
                localCacheConfig,
                distributedCacheConfig,
                zone,
                keySerde,
                valueSerde,
                batchDesiredBytes,
                batchMaxDuration,
                repartitionName,
                partitioner,
                staticOutputPartitionsToRacks,
                kafkaRackAssignmentRefreshInterval,
                new SimpleMeterRegistry());
    }

    /**
     * Creates a new S3Shuffler with full configuration.
     *
     * @param s3Config S3 client and bucket configuration
     * @param localCacheConfig local (per-instance) cache configuration
     * @param distributedCacheConfig distributed (per-AZ) cache configuration
     * @param zone the availability zone identifier of this instance
     * @param keySerde the key serde
     * @param valueSerde the value serde
     * @param batchDesiredBytes the target batch size in bytes
     * @param batchMaxDuration the maximum time before a batch is finalized
     * @param repartitionName name for the internal repartitioning topic
     * @param partitioner the partitioning strategy
     * @param staticOutputPartitionsToRacks static mapping of output partition numbers to rack IDs
     * @param kafkaRackAssignmentRefreshInterval interval for refreshing the partition-to-rack mapping
     * @param meterRegistry the meter registry for metrics
     */
    public S3Shuffler(
            S3Config s3Config,
            LocalCacheConfig localCacheConfig,
            DistributedCacheConfig distributedCacheConfig,
            String zone,
            Serde<K> keySerde,
            Serde<V> valueSerde,
            long batchDesiredBytes,
            Duration batchMaxDuration,
            String repartitionName,
            Partitioner partitioner,
            Map<Integer, String> staticOutputPartitionsToRacks,
            Duration kafkaRackAssignmentRefreshInterval,
            MeterRegistry meterRegistry) {
            //Refresher rackAssignmentRefresher,
            //String applicationId, // TODO Try to avoid passing this
            //String bootstrapServers) { // TODO Try to avoid passing this
        this.s3BatchFacade = localCacheConfig.maybeWrapS3BatchFacade(
                distributedCacheConfig.maybeWrapS3BatchFacade(
                        s3Config.toS3BatchFacade(meterRegistry),
                        zone,
                        batchDesiredBytes,
                        meterRegistry),
                batchDesiredBytes,
                meterRegistry);
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.batchDesiredBytes = batchDesiredBytes;
        this.batchMaxDuration = batchMaxDuration;
        this.repartitionName = repartitionName;
        //this.applicationId = applicationId;
        //Properties adminProperties = new Properties();
        //adminProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        //this.adminClient = AdminClient.create(adminProperties);
        this.outputPartitioner = new OutputPartitioner(partitioner, staticOutputPartitionsToRacks);
        if (!kafkaRackAssignmentRefreshInterval.equals(Duration.ZERO)) {
            this.rackAssignmentRefresher = Refresher.create(kafkaRackAssignmentRefreshInterval, this::refreshPartitionRackAssignment);
        } else {
            this.rackAssignmentRefresher = Refresher.NOOP;
        }
        this.meterRegistry = meterRegistry;
    }

    /**
     * Returns a supplier for the Batcher processor.
     *
     * @return the batcher processor supplier
     */
    public ProcessorSupplier<K, V, NotificationDestination, NotificationDetails> batcherSupplier() {
        return new S3BatcherProcessor.Supplier<>(
                s3BatchFacade,
                outputPartitioner,
                keySerde,
                valueSerde,
                batchDesiredBytes,
                batchMaxDuration,
                taskSharedBatcher,
                this::handleProcessorInit,
                meterRegistry);
    }

    /**
     * Returns a supplier for the Debatcher processor.
     *
     * @return the debatcher processor supplier
     */
    public ProcessorSupplier<NotificationDestination, NotificationDetails, K, V> debatcherSupplier() {
        return new S3DebatcherProcessor.Supplier<>(s3BatchFacade, keySerde, valueSerde, meterRegistry);
    }

    /**
     * Returns the {@link Repartitioned} configuration for the internal notification repartitioning
     * step. Use with {@code .repartition(shuffler.repartitioned())}.
     *
     * @return the repartitioned configuration
     */
    public Repartitioned<NotificationDestination, NotificationDetails> repartitioned() {
        return Repartitioned
                .with(new NotificationDestinationSerde(), new NotificationDetailsSerde())
                .withName(repartitionName)
                .withStreamPartitioner(new NotificationStreamPartitioner());
    }

    /**
     * Returns the {@link Produced} configuration for writing notifications to a Kafka topic
     * in split topologies.
     *
     * @return the produced configuration
     */
    public Produced<NotificationDestination, NotificationDetails> produced() {
        return Produced
                .with(new NotificationDestinationSerde(), new NotificationDetailsSerde())
                //.withName(repartitionName)
                .withStreamPartitioner(new NotificationStreamPartitioner());
    }

    /**
     * Returns the {@link Consumed} configuration for reading notifications from a Kafka topic
     * in split topologies.
     *
     * @return the consumed configuration
     */
    public Consumed<NotificationDestination, NotificationDetails> consumed() {
        return Consumed
                .with(new NotificationDestinationSerde(), new NotificationDetailsSerde());
                //.withName(repartitionName)

    }

    private void handleProcessorInit(ProcessorContext<?, ?> context) {
        System.out.println("Refresh partition assignment.");
        this.initRefresherOrFail(
                (List<String>) context.appConfigs().get(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG),
                context.applicationId());
        this.rackAssignmentRefresher.runOnce();

        // TODO This seems to be missing:
        //this.rackAssignmentRefresher.start();
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


    @Override
    public void close() {
        this.rackAssignmentRefresher.close();
        this.adminClient.close();
        this.s3BatchFacade.close();
    }
}
