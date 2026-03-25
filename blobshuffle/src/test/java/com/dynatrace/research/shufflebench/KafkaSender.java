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

import com.dynatrace.research.shufflebench.record.Record;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.dynatrace.research.shufflebench.record.RecordSerializer;

import java.io.Closeable;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.function.Consumer;

public class KafkaSender implements Consumer<Record>, Closeable {

  private static final Logger LOGGER = LogManager.getLogger(KafkaSender.class);

  private final KafkaProducer<Void, Record> kafkaProducer;

  private final String topic;

  private final Consumer<Record> callback; // May be null

  public KafkaSender(final String bootstrapServers, Map<String, String> kafkaProducerConfig, final String topic) {
    this(bootstrapServers, topic, kafkaProducerConfig, null);
  }

  public KafkaSender(final String bootstrapServers, final String topic, Map<String, String> kafkaProducerConfig, final Consumer<Record> callback) {
    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.putAll(kafkaProducerConfig);
    this.kafkaProducer = new KafkaProducer<>(properties, new NullKeySerializer(), new RecordSerializer());
    this.topic = topic;
    this.callback = callback;
  }

  @Override
  public void accept(Record record) {
    ProducerRecord<Void, Record> producerRecord = new ProducerRecord<>(this.topic, record);
    LOGGER.debug("Send record to Kafka: {}", producerRecord);
    Future<RecordMetadata> result = this.produceKafkaRecord(producerRecord);
  }

  private Future<RecordMetadata> produceKafkaRecord(ProducerRecord<Void, Record> producerRecord) {
    if (callback == null) {
      return this.kafkaProducer.send(producerRecord);
    } else {
      return this.kafkaProducer.send(producerRecord, (metadata, exception) -> callback.accept(producerRecord.value()));
    }
  }

  @Override
  public void close() {
    this.kafkaProducer.close();
  }
}
