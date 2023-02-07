/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.redis;

import com.github.jcustenborder.kafka.connect.utils.VersionUtil;
import io.lettuce.core.LettuceFutures;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

public abstract class AbstractRedisSinkTask<CONFIG extends RedisConnectorConfig> extends SinkTask {
  private static final Logger log = LoggerFactory.getLogger(AbstractRedisSinkTask.class);
  protected List<CompletableFuture<?>> futures;
  protected CONFIG config;
  protected RedisSessionFactory sessionFactory = new RedisSessionFactoryImpl();
  protected RedisSession session;

  protected abstract CONFIG config(Map<String, String> settings);

  @Override
  public String version() {
    return VersionUtil.version(this.getClass());
  }

  @Override
  public void start(Map<String, String> settings) {
    this.config = config(settings);
    this.session = this.sessionFactory.createSession(this.config);
    this.futures = new ArrayList<>(64000);

    final Set<TopicPartition> assignment = this.context.assignment();
    if (!assignment.isEmpty()) {
      final Map<TopicPartition, Long> offsets = new ConcurrentHashMap<>(assignment.size() * 2);
      retrieveOffsets(assignment, offsets);
      retrieveLegacyOffsets(assignment, offsets);

      //TODO: FIX offsetReset
//      if (RedisSinkConnectorConfig.OffsetReset.Earliest == this.config.offsetReset) {
//        for (TopicPartition topicPartition : assignment) {
//          if (!offsets.containsKey(topicPartition)) {
//            log.debug("start() - Requesting offset 0 for {}", topicPartition);
//            offsets.put(topicPartition, 0L);
//          }
//        }
//      }

      if (!offsets.isEmpty()) {
        log.debug("start() - Requesting offsets\n{}", offsets);
        this.context.offset(offsets);
      }
    }
  }

  protected byte[] toBytes(String source, Object input) {
    final byte[] result;

    if (input instanceof String) {
      String s = (String) input;
      result = s.getBytes(this.config.charset);
    } else if (input instanceof byte[]) {
      result = (byte[]) input;
    } else if (null == input) {
      result = null;
    } else {
      throw new DataException(
          String.format(
              "The %s for the record must be String or Bytes. Consider using the ByteArrayConverter " +
                  "or StringConverter if the data is stored in Kafka in the format needed in Redis. " +
                  "Another option is to use a single message transformation to transform the data before " +
                  "it is written to Redis.",
              source
          )
      );
    }

    return result;
  }

  protected <T> Function<Throwable, T> exceptionally(SinkRecord record) {
    return t -> {
      context.errantRecordReporter().report(record, t);
      return null;
    };
  }

  void retrieveOffsets(Set<TopicPartition> assignment, Map<TopicPartition, Long> offsets) {
    Set<String> topics = assignment.stream()
        .map(TopicPartition::topic)
        .collect(Collectors.toSet());

    final AtomicInteger errorCount = new AtomicInteger(0);
    CompletableFuture<?>[] futures = topics.stream()
        .map(
            topic -> {
              String topicKey = String.format("__kafka.offsets.%s", topic);
              log.debug("retrieveOffsets() - Calling hgetall('{}')", topicKey);
              return this.session.hash().hgetall(topicKey.getBytes(StandardCharsets.UTF_8))
                  .thenAccept(value -> {
                    log.debug("retrieveOffsets() - topic = '{}'", topic);
                    if (null == value || value.isEmpty()) {
                      log.debug("retrieveOffsets() - topic = '{}' no offsets found.", topic);
                      return;
                    }

                    Map<TopicPartition, Long> topicOffsets = value.entrySet().stream()
                        .collect(Collectors.toMap(
                            entry -> new TopicPartition(topic, Integer.parseInt(new String(entry.getKey(), StandardCharsets.UTF_8))),
                            entry -> Long.parseLong(new String(entry.getValue(), StandardCharsets.UTF_8))
                        ));
                    log.debug("retrieveOffsets() - All offsets for {}\n{}", topic, topicOffsets);
                    topicOffsets.forEach(((topicPartition, offset) -> {
                      if (assignment.contains(topicPartition)) {
                        log.debug("retrieveOffsets() - adding offset {}: {}", topicPartition, offset);
                        offsets.put(topicPartition, offset);
                      }
                    }));
                  }).exceptionally(ex -> {
                    log.error("Exception thrown calling hgetall('{}}')", topic, ex);
                    errorCount.incrementAndGet();
                    return null;
                  }).toCompletableFuture();
            })
        .toArray(CompletableFuture<?>[]::new);
    LettuceFutures.awaitAll(30, TimeUnit.SECONDS, futures);

    if (errorCount.get() > 0) {
      throw new IllegalStateException(); //TODO: Clean this up.
    }

  }

  void retrieveLegacyOffsets(Set<TopicPartition> assignment, Map<TopicPartition, Long> offsets) {
    final AtomicInteger errorCount = new AtomicInteger(0);

    CompletableFuture<?>[] futures = assignment.stream()
        .filter(o -> !offsets.containsKey(o))
        .map(topicPartition -> {
          String topicPartitionKey = String.format("__kafka.offset.%s.%s", topicPartition.topic(), topicPartition.partition());
          log.debug("retrieveLegacyOffsets() - Calling get('{}')", topicPartitionKey);
          return this.session.string().get(topicPartitionKey.getBytes(StandardCharsets.UTF_8))
              .thenAccept(b -> {
                if (null != b) {
                  String value = new String(b, StandardCharsets.UTF_8);
                  Long offset = Long.parseLong(value);
                  log.debug("retrieveLegacyOffsets() - Adding offset {}:{}", topicPartition, offset);
                  offsets.put(topicPartition, offset);
                } else {
                  log.trace("retrieveLegacyOffsets() - No value found for get('{}')", topicPartitionKey);
                }
              }).exceptionally(ex -> {
                errorCount.incrementAndGet();
                log.error("Exception thrown calling get('{}')", topicPartitionKey, ex);
                return null;
              }).toCompletableFuture();
        }).toArray(CompletableFuture<?>[]::new);

    LettuceFutures.awaitAll(30, TimeUnit.SECONDS, futures);
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    Map<String, Map<String, String>> partitions = new LinkedHashMap<>();
    currentOffsets.forEach((topicPartition, offsetMetadata) -> {
      String topicKey = String.format("__kafka.offsets.%s", topicPartition.topic());
      Map<String, String> offsets = partitions.computeIfAbsent(topicKey, s -> new LinkedHashMap<>());
      offsets.put(
          Integer.toString(topicPartition.partition()),
          Long.toString(offsetMetadata.offset())
      );
    });

    Map<String, Map<byte[], byte[]>> topicToPartitionAndOffsets = new HashMap<>();

    currentOffsets.forEach(((topicPartition, offsetAndMetadata) -> {
      Map<byte[], byte[]> partitionsAndOffsets = topicToPartitionAndOffsets.computeIfAbsent(topicPartition.topic(), k -> new HashMap<>());
      partitionsAndOffsets.put(
          Integer.toString(topicPartition.partition()).getBytes(StandardCharsets.UTF_8),
          Long.toString(offsetAndMetadata.offset()).getBytes(StandardCharsets.UTF_8)
      );
    }));

    topicToPartitionAndOffsets.forEach((topic, offsets) -> {
      byte[] topicKey = String.format("__kafka.offsets.%s", topic).getBytes(StandardCharsets.UTF_8);
      CompletableFuture<?> future = this.session.hash().hset(topicKey, offsets).toCompletableFuture();
      this.futures.add(future);
    });

    this.session.flushCommands();
    log.debug("flush() - Waiting for {} commands to complete.", futures.size());
    LettuceFutures.awaitAll(
        30000,
        TimeUnit.MILLISECONDS,
        this.futures.toArray(new CompletableFuture[futures.size()])
    );
    this.futures.clear();
  }

  @Override
  public void stop() {
    try {
      this.session.close();
    } catch (Exception e) {
      log.error("Exception while closing session", e);
    }
  }
}
