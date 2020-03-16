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
import com.github.jcustenborder.kafka.connect.utils.data.SinkOffsetState;
import com.github.jcustenborder.kafka.connect.utils.jackson.ObjectMapperFactory;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisFuture;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public abstract class BaseRedisSinkTask<CONFIG extends RedisConnectorConfig> extends SinkTask {
  private static final Logger log = LoggerFactory.getLogger(BaseRedisSinkTask.class);
  protected CONFIG config;
  protected RedisClusterSession<byte[], byte[]> session;
  RedisSessionFactory sessionFactory = new RedisSessionFactoryImpl();

  static SinkOffsetState state(KeyValue<byte[], byte[]> input) {
    if (!input.hasValue()) {
      return null;
    }
    try {
      return ObjectMapperFactory.INSTANCE.readValue(input.getValue(), SinkOffsetState.class);
    } catch (IOException e) {
      throw new DataException(e);
    }
  }

  static String formatLocation(SinkRecord record) {
    return String.format(
        "topic = %s partition = %s offset = %s",
        record.topic(),
        record.kafkaPartition(),
        record.kafkaOffset()
    );
  }

  private static String redisOffsetKey(TopicPartition topicPartition) {
    return String.format("__kafka.offset.%s.%s", topicPartition.topic(), topicPartition.partition());
  }

  @Override
  public String version() {
    return VersionUtil.version(this.getClass());
  }

  protected abstract CONFIG config(Map<String, String> settings);

  @Override
  public void start(Map<String, String> settings) {
    this.config = config(settings);
    this.session = this.sessionFactory.createClusterSession(this.config);

    final Set<TopicPartition> assignment = this.context.assignment();
    if (!assignment.isEmpty()) {
      final byte[][] partitionKeys = assignment.stream()
          .map(BaseRedisSinkTask::redisOffsetKey)
          .map(s -> s.getBytes(this.config.charset))
          .toArray(byte[][]::new);

      final RedisFuture<List<KeyValue<byte[], byte[]>>> partitionKeyFuture = this.session.asyncCommands().mget(partitionKeys);
      final List<SinkOffsetState> sinkOffsetStates;
      try {
        final List<KeyValue<byte[], byte[]>> partitionKey = partitionKeyFuture.get(this.config.operationTimeoutMs, TimeUnit.MILLISECONDS);
        sinkOffsetStates = partitionKey.stream()
            .map(BaseRedisSinkTask::state)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        throw new RetriableException(e);
      }
      Map<TopicPartition, Long> partitionOffsets = new HashMap<>(assignment.size());
      for (SinkOffsetState state : sinkOffsetStates) {
        partitionOffsets.put(state.topicPartition(), state.offset());
        log.info("Requesting offset {} for {}", state.offset(), state.topicPartition());
      }
      for (TopicPartition topicPartition : assignment) {
        if (!partitionOffsets.containsKey(topicPartition)) {
          partitionOffsets.put(topicPartition, 0L);
          log.info("Requesting offset {} for {}", 0L, topicPartition);
        }
      }
      this.context.offset(partitionOffsets);
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

  protected abstract void operations(SinkOperations sinkOperations, Collection<SinkRecord> records);

  @Override
  public void put(Collection<SinkRecord> records) {
    log.debug("put() - Processing {} record(s)", records.size());
    SinkOperations sinkOperations = new SinkOperations(this.config);
    operations(sinkOperations, records);
    sinkOperations.execute(this.session.asyncCommands());
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

    partitions.forEach((topicKey, offsets) -> {
      final byte[] key = topicKey.getBytes(this.config.charset);
      offsets.forEach((partition, offset) -> {
        byte[] partitionKey = partition.getBytes(this.config.charset);
        byte[] offsetBytes = offset.getBytes();
        RedisFuture<Boolean> future = this.session.asyncCommands().hset(key, partitionKey, offsetBytes);

      });
    });
  }

  @Override
  public void stop() {
    try {
      if (null != this.session) {
        this.session.close();
      }
    } catch (Exception e) {
      log.warn("Exception thrown", e);
    }
  }
}
