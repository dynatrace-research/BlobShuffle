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
package com.dynatrace.research.shufflebench;

import com.dynatrace.hash4j.hashing.Hashing;
import com.dynatrace.research.shufflebench.record.RandomRecordGenerator;
import com.dynatrace.research.shufflebench.record.Record;
import io.smallrye.config.SmallRyeConfig;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class KafkaLoadGenerator {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaLoadGenerator.class);

  private final String seedString;

  private final String kafkaBootstrapServers;

  private final String kafkaTopic;

  private final int numSources;

  private final boolean shareKafkaSender;

  private final int recordsPerSecondAndSource;

  private final int recordSizeInBytes;

  private final Map<String, String> kafkaProducerConfig;

  private final ScheduledExecutorService executor;

  private final Consumer<Record> callback; // May be null

  private final List<KafkaSender> openKafkaSenders = new ArrayList<>();
  private final List<RecordSource> openRecordSources = new ArrayList<>();


  public KafkaLoadGenerator(
          String seedString,
          String kafkaBootstrapServers,
          String kafkaTopic,
          int numSources,
          boolean shareKafkaSender,
          int recordsPerSecondAndSource,
          int recordSizeInBytes,
          int threadPoolSize,
          Map<String, String> kafkaProducerConfig
  ) {
    this(
            seedString,
            kafkaBootstrapServers,
            kafkaTopic,
            numSources,
            shareKafkaSender,
            recordsPerSecondAndSource,
            recordSizeInBytes,
            threadPoolSize,
            kafkaProducerConfig,
            null);
  }

  public KafkaLoadGenerator(
          String seedString,
          String kafkaBootstrapServers,
          String kafkaTopic,
          int numSources,
          boolean shareKafkaSender,
          int recordsPerSecondAndSource,
          int recordSizeInBytes,
          int threadPoolSize,
          Map<String, String> kafkaProducerConfig,
          Consumer<Record> callback
  ) {
    this.seedString = seedString;
    this.kafkaBootstrapServers = kafkaBootstrapServers;
    this.kafkaTopic = kafkaTopic;
    this.numSources = numSources;
    this.shareKafkaSender = shareKafkaSender;
    this.recordsPerSecondAndSource = recordsPerSecondAndSource;
    this.recordSizeInBytes = recordSizeInBytes;
    this.executor = new ScheduledThreadPoolExecutor(threadPoolSize);
    this.kafkaProducerConfig = kafkaProducerConfig;
    this.callback = callback;
  }

  public void startAsync() {
    final List<KafkaSender> kafkaSenders = IntStream.range(0, this.shareKafkaSender ? 1 : numSources)
            .mapToObj(i -> new KafkaSender(this.kafkaBootstrapServers, this.kafkaTopic, this.kafkaProducerConfig, this.callback))
            .collect(Collectors.toList());
    for (int sourceId = 0; sourceId < numSources; sourceId++) {
      final long seed = Hashing.komihash4_3().hashStream().putString(seedString).putInt(sourceId).getAsLong();
      final RecordSource recordSource = new RecordSource(
              executor,
              this.recordsPerSecondAndSource,
              this.shareKafkaSender ? kafkaSenders.get(0) : kafkaSenders.get(sourceId),
              // new StaticRecordGenerator(),
              new RandomRecordGenerator(seed, recordSizeInBytes),
              "source" + sourceId);
      openRecordSources.add(recordSource);
    }
    openKafkaSenders.addAll(kafkaSenders);
  }

  public void startBlocking(int executionTimeMs) throws InterruptedException {
    this.startAsync();
    Thread.sleep(executionTimeMs);
  }

  public void stop() {
    for (RecordSource recordSource : openRecordSources) {
      recordSource.stop();
    }
    openRecordSources.clear();
    for (KafkaSender kafkaSender : openKafkaSenders) {
      kafkaSender.close();
    }
    openKafkaSenders.clear();
    executor.shutdown();
    try {
      executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public static void main(String[] args) throws InterruptedException {
    final SmallRyeConfig config = ConfigProvider.getConfig().unwrap(SmallRyeConfig.class);
    final KafkaLoadGenerator kafkaLoadGenerator = new KafkaLoadGenerator(
            config.getValue("seed.string", String.class),
            config.getValue("kafka.bootstrap.servers", String.class),
            config.getValue("kafka.topic", String.class),
            config.getValue("num.sources", Integer.class),
            config.getValue("share.kafka.producer.among.sources", Boolean.class),
            config.getValue("num.records.per.source.second", Integer.class),
            config.getValue("record.size.bytes", Integer.class),
            config.getValue("thread.pool.size", Integer.class),
            config.getOptionalValues("kafka.producer", String.class, String.class).orElse(Map.of())
    );
    int executionTimeMs = config.getValue("execution.time.ms", Integer.class);
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      LOGGER.info("Shut down load generator.");
      kafkaLoadGenerator.stop();
    }));
    kafkaLoadGenerator.startBlocking(executionTimeMs);
    kafkaLoadGenerator.stop();
  }
}
