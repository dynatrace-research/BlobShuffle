# BlobShuffle

**Cost-effective repartitioning for Kafka Streams via cloud object storage**

BlobShuffle is a novel approach to shuffling (repartitioning) in stream processing systems that leverages cloud object storage (e.g., Amazon S3) as an intermediate exchange layer.
Instead of sending all shuffled records directly through Kafka, BlobShuffle groups records into batches, stores these batches in object storage, and forwards only compact notifications via Kafka.
Downstream operators use these notifications to retrieve the relevant batches and extract the corresponding records.

BlobShuffle is implemented as an add-on for [Apache Kafka Streams](https://kafka.apache.org/documentation/streams/) that requires only minimal code changes to existing applications, leaves Kafka and the underlying infrastructure unmodified, and preserves Kafka Streams' consistency and correctness guarantees.


## Motivation

In cloud-based deployments, shuffling can become a major cost bottleneck for stateful stream processing workloads.
Stream processing systems are typically deployed across multiple availability zones (AZs) for fault tolerance.
Repartitioning causes continuous cross-AZ traffic, incurring significant charges on cloud providers that often dominate the operational cost of stateful workloads.

In Kafka Streams, shuffling is implemented via dedicated *repartitioning topics* hosted by the Kafka brokers.
Each shuffle step writes records to a repartitioning topic, from which downstream tasks then read.
This design amplifies two key challenges:

- **Cloud costs.** Shuffling causes cross-AZ traffic when records are written to repartitioning topic partitions hosted in different AZs. In addition, these topics are typically replicated across AZs, further increasing cross-AZ traffic and, consequently, cloud costs.
- **Operational complexity.** Operating Apache Kafka at large scale is inherently complex. High shuffle volumes increase throughput and storage pressure on Kafka, exacerbating these operational challenges.

BlobShuffle addresses both challenges by offloading the bulk data transfer to cloud object storage, which is region-wide and does not incur explicit cross-AZ charges, while still using Kafka for lightweight coordination via compact notification messages.


## Paper

BlobShuffle is described in detail in the following paper:

> Sören Henning, Otmar Ertl, Adriano Vogel. **BlobShuffle: Cost-Effective Repartitioning in Stream Processing Systems via Object Storage Exemplified with Kafka Streams.** [Preprint](https://arxiv.org/abs/2606.03364).


## Architecture Overview

BlobShuffle consists of two main operators—**Batcher** and **Debatcher**—that wrap the standard Kafka Streams repartitioning step:

### Write Path (Batcher)

1. The Batcher computes the destination partition for each incoming record.
2. Records are serialized and accumulated in per-partition buffers, grouped by target availability zone.
3. When a batch is complete (by size, time, or commit), the concatenated buffer is uploaded to object storage (e.g., S3) as a single object.
4. For each partition that contributed records to the batch, the Batcher sends a compact notification via the repartitioning topic containing the batch identifier and byte range.

### Read Path (Debatcher)

1. The Debatcher reads notifications from the repartitioning topic.
2. For each notification, it fetches the referenced batch (or sub-batch) from object storage.
3. It deserializes the contained records and forwards them to downstream operators.

### Caching

All access to object storage is routed through a multi-layered caching mechanism:

- **Distributed cache:** Per-AZ in-memory LRU cache shared across stream processing instances. Ensures each batch is downloaded from object storage at most once per AZ (unless the cache entry has been evicted).
- **Local cache** (optional): Per-instance in-memory LRU cache that avoids repeated lookups to the distributed cache for batches accessed by multiple operators on the same instance.


## Quick Start

### Integration

Integrating BlobShuffle into an existing Kafka Streams application requires only minimal code changes.
The original repartition step is replaced by BlobShuffle's Batcher and Debatcher operators along with a repartitioning step configured by BlobShuffle:

```java
import com.dynatrace.research.blobshuffle.*;
import com.dynatrace.research.blobshuffle.kstreams.BlobShuffle;

// Configure BlobShuffle
BlobShuffle<MyKey, MyValue> shuffler = new BlobShuffle<>(
    new S3Config(s3Client, "my-bucket-name"),
    LocalCacheConfig.disabled(),                 // or: LocalCacheConfig.enabled(cacheSizeBytes, cacheOnWrite)
    DistributedCacheConfig.disabled(),            // or: DistributedCacheConfig.enabled(cacheSizeBytes, cacheOnWrite, advertisedHost, port)
    System.getenv("TOPOLOGY_ZONE"),              // availability zone of this instance
    Serdes.MyKey(),                              // key serde
    Serdes.MyValue(),                            // value serde
    16 * 1024 * 1024,                            // desired batch size (bytes)
    Duration.ofSeconds(5),                       // max batching duration
    "my-repartition",                            // repartition topic name
    Duration.ofSeconds(300));                    // rack assignment refresh interval

// Build topology — replace the standard .repartition() call
StreamsBuilder builder = new StreamsBuilder();
builder.stream("input-topic")
    .selectKey(...)
    .process(shuffler.batcherSupplier())         // ← Batcher
    .repartition(shuffler.repartitioned())       // ← Repartitioning (notifications only)
    .process(shuffler.debatcherSupplier())       // ← Debatcher
    .aggregate(...);
```


### Prerequisites

- **Java 25** or later (the project uses a Java 25 toolchain)
- **Apache Kafka 3.9+** (broker and client)
- **Amazon S3** or an S3-compatible object storage service
- **AWS credentials** configured (e.g., via environment variables, IAM roles, or the default credential provider chain)

### Building from Source

The project uses Gradle with the included wrapper:

```bash
# Build the project
./gradlew build

# Run tests (requires Docker for Testcontainers-based integration tests)
./gradlew test

# Generate Javadoc
./gradlew javadoc

# Check and apply code style and formatting (e.g., license headers)
./gradlew spotlessCheck
./gradlew spotlessApply
```


## Configuration Reference

### `BlobShuffle` Constructor Parameters

| Parameter | Type | Description |
|---|---|---|
| `s3Config` | `S3Config` | S3 client and bucket name (`new S3Config(s3Client, bucketName)`) |
| `localCacheConfig` | `LocalCacheConfig` | Local (per-instance) LRU cache configuration. Use `LocalCacheConfig.disabled()` to disable. |
| `distributedCacheConfig` | `DistributedCacheConfig` | Distributed (per-AZ) LRU cache configuration. Use `DistributedCacheConfig.disabled()` to disable. |
| `zone` | `String` | The availability zone identifier for this stream processing instance. Used for AZ-aware batching and cache routing. |
| `keySerde` | `Serde<K>` | Kafka Streams serde for record keys. |
| `valueSerde` | `Serde<V>` | Kafka Streams serde for record values. |
| `batchDesiredBytes` | `long` | Target batch size in bytes. Larger batches reduce S3 request costs but increase latency. |
| `batchMaxDuration` | `Duration` | Maximum time before a batch is finalized, regardless of size. Bounds worst-case latency. |
| `repartitionName` | `String` | Name for the internal Kafka repartitioning topic used for notifications. |
| `partitioner` | `Partitioner` | *(Optional)* Partitioning strategy. Defaults to `Partitioner.KAFKA_BUILTIN`. |
| `staticOutputPartitionsToRacks` | `Map<Integer, String>` | *(Optional)* Static mapping of output partition numbers to rack IDs. Use when rack assignments are known upfront and dynamic refresh is not needed. |
| `kafkaRackAssignmentRefreshInterval` | `Duration` | Interval for refreshing the partition-to-rack mapping from Kafka. Set to `Duration.ZERO` to disable. |
| `meterRegistry` | `MeterRegistry` | *(Optional)* Micrometer registry for metrics collection. Defaults to a `SimpleMeterRegistry`. |

### Cache Configuration

- **`LocalCacheConfig.disabled()`** — disables the local cache.
- **`LocalCacheConfig.enabled(sizeInBytes, cacheOnWrite)`** — enables the per-instance LRU cache with the given capacity (bytes) and write-through behavior.
- **`DistributedCacheConfig.disabled()`** — disables the distributed cache.
- **`DistributedCacheConfig.enabled(sizeInBytes, cacheOnWrite, advertisedHost, port)`** — enables the per-AZ distributed LRU cache with the given capacity, write-through behavior, and the host/port this instance advertises to other cache cluster members.

### Example `application.properties`

The included example application (`KafkaStreamsBlobShuffleExample`) uses [MicroProfile Config](https://microprofile.io/specifications/microprofile-config/) and can be configured via `application.properties` or environment variables:

```properties
topology=Full
kafka.bootstrap.servers=localhost:9092
kafka.topic.input=input
kafka.topic.notifications=notifications
kafka.topic.output=output
s3.bucket=blobshuffle
batch.desired.bytes=16777216
batch.max.ms=5000
partitioner=KAFKA_BUILTIN
cache.local.enabled=false
cache.local.size.bytes=536870912
cache.local.on.write=true
cache.distributed.enabled=true
cache.distributed.size.bytes=4294967296
cache.distributed.on.write=true
cache.distributed.advertised.host=localhost
cache.distributed.port=7070
```

Kafka Streams properties can be set with the prefix `kafkastreams.`, e.g., `kafkastreams."commit.interval.ms"=30000`.


## Public API

BlobShuffle's integration with Kafka Streams is provided through three methods on the `BlobShuffle` instance:

| Method | Returns | Description |
|---|---|---|
| `batcherSupplier()` | `ProcessorSupplier<K, V, ...>` | Supplier for the Batcher processor. Use with `.process()`. |
| `debatcherSupplier()` | `ProcessorSupplier<..., K, V>` | Supplier for the Debatcher processor. Use with `.process()`. |
| `repartitioned()` | `Repartitioned<..., ...>` | Configuration for the repartitioning step. Use with `.repartition()`. |

For split topologies (separate Batcher and Debatcher sub-topologies):

| Method | Returns | Description |
|---|---|---|
| `produced()` | `Produced<..., ...>` | Configuration for writing notifications to a Kafka topic. |
| `consumed()` | `Consumed<..., ...>` | Configuration for reading notifications from a Kafka topic. |


## Benchmarking and Evaluation

Instructions for setting up the cloud-native benchmarking environment, running experiments, and reproducing our paper's evaluation are available in the [benchmarking/](benchmarking/) directory.


## Notice

BlobShuffle is a research prototype developed at [Dynatrace Research](https://www.dynatrace.com/research/).
It is **not** an officially supported Dynatrace product.

This repository is available under the terms in [LICENSE](LICENSE).
Use is limited to internal, non-production, non-commercial purposes.
Redistribution, sublicensing, modification, sale, and use for training or improving AI/ML models are not permitted.
