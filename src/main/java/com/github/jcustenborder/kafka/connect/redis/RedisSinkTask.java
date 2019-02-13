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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.jcustenborder.kafka.connect.utils.VersionUtil;
import com.github.jcustenborder.kafka.connect.utils.data.SinkOffsetState;
import com.github.jcustenborder.kafka.connect.utils.data.TopicPartitionCounter;
import com.github.jcustenborder.kafka.connect.utils.jackson.ObjectMapperFactory;
import com.google.common.base.Charsets;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class RedisSinkTask extends SinkTask {
  private static final Logger log = LoggerFactory.getLogger(RedisSinkTask.class);

  @Override
  public String version() {
    return VersionUtil.version(this.getClass());
  }

  RedisSinkConnectorConfig config;
  RedisSession session;

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


  @Override
  public void start(Map<String, String> settings) {
    this.config = new RedisSinkConnectorConfig(settings);
    this.session = RedisSessionImpl.create(this.config);

    final Set<TopicPartition> assignment = this.context.assignment();
    final byte[][] partitionKeys = assignment.stream()
        .map(RedisSinkTask::redisOffsetKey)
        .map(s -> s.getBytes(Charsets.UTF_8))
        .toArray(byte[][]::new);


    final RedisFuture<List<KeyValue<byte[], byte[]>>> partitionKeyFuture = this.session.asyncCommands().mget(partitionKeys);
    final List<SinkOffsetState> sinkOffsetStates;
    try {
      final List<KeyValue<byte[], byte[]>> partitionKey = partitionKeyFuture.get(this.config.operationTimeoutMs, TimeUnit.MILLISECONDS);
      sinkOffsetStates = partitionKey.stream()
          .map(RedisSinkTask::state)
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

  private byte[] toBytes(String source, Object input) {
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

  static String formatLocation(SinkRecord record) {
    return String.format(
        "topic = %s partition = %s offset = %s",
        record.topic(),
        record.kafkaPartition(),
        record.kafkaOffset()
    );
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    log.debug("put() - Processing {} record(s)", records.size());
    List<SinkOperation> operations = new ArrayList<>(records.size());

    SinkOperation operation = SinkOperation.NONE;

    TopicPartitionCounter counter = new TopicPartitionCounter();

    for (SinkRecord record : records) {
      log.trace("put() - Processing record " + formatLocation(record));
      if (null == record.key()) {
        throw new DataException(
            "The key for the record cannot be null. " + formatLocation(record)
        );
      }
      final byte[] key = toBytes("key", record.key());
      if (null == key || key.length == 0) {
        throw new DataException(
            "The key cannot be an empty byte array. " + formatLocation(record)
        );
      }

      final byte[] value = toBytes("value", record.value());

      SinkOperation.Type currentOperationType;

      if (null == value) {
        currentOperationType = SinkOperation.Type.DELETE;
      } else {
        currentOperationType = SinkOperation.Type.SET;
      }

      if (currentOperationType != operation.type) {
        log.debug(
            "put() - Creating new operation. current={} last={}",
            currentOperationType,
            operation.type
        );
        operation = SinkOperation.create(currentOperationType, this.config, records.size());
        operations.add(operation);
      }
      operation.add(key, value);
      counter.increment(record.topic(), record.kafkaPartition(), record.kafkaOffset());
    }

    log.debug(
        "put() - Found {} operation(s) in {} record{s}. Executing operations...",
        operations.size(),
        records.size()
    );

    final List<SinkOffsetState> offsetData = counter.offsetStates();
    operation = SinkOperation.create(SinkOperation.Type.SET, this.config, offsetData.size());
    operations.add(operation);
    for (SinkOffsetState e : offsetData) {
      final byte[] key = String.format("__kafka.offset.%s.%s", e.topic(), e.partition()).getBytes(Charsets.UTF_8);
      final byte[] value;
      try {
        value = ObjectMapperFactory.INSTANCE.writeValueAsBytes(e);
      } catch (JsonProcessingException e1) {
        throw new DataException(e1);
      }
      operation.add(key, value);
      log.trace("put() - Setting offset: {}", e);
    }

    for (SinkOperation op : operations) {
      log.debug("put() - Executing {} operation with {} values", op.type, op.size());
      try {
        op.execute(this.session.asyncCommands());
      } catch (InterruptedException e) {
        throw new RetriableException(e);
      }
    }
  }

  private static String redisOffsetKey(TopicPartition topicPartition) {
    return String.format("__kafka.offset.%s.%s", topicPartition.topic(), topicPartition.partition());
  }

  @Override
  public void stop() {
    try {
      this.session.close();
    } catch (Exception e) {
      log.warn("Exception thrown", e);
    }
  }
}
