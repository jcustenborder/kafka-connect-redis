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
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RedisSinkTask extends SinkTask {
  private static final Logger log = LoggerFactory.getLogger(RedisSinkTask.class);

  @Override
  public String version() {
    return VersionUtil.version(this.getClass());
  }

  RedisSinkConnectorConfig config;
  RedisSessionFactory sessionFactory = new RedisSessionFactoryImpl();
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
    this.session = this.sessionFactory.create(this.config);

    final Set<TopicPartition> assignment = this.context.assignment();
    if (!assignment.isEmpty()) {
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
  }

    @Override
    public void put(Collection <SinkRecord> records) {
      log.debug("put() - Processing {} record(s)", records.size());
      SinkOperation deleteOp = SinkOperation.create(SinkOperation.Type.DELETE, this.config, records.size());
      Stream<RedisRecord> deletes = records
              .stream()
              .filter(r -> r.value() == null)
              .map(r -> RedisRecord.fromDeleteRecord(r, this.config));
      deletes.forEach(d -> deleteOp.add(d.key(), d.value()));

      SinkOperation setOp = SinkOperation.create(SinkOperation.Type.SET, this.config, records.size());
      Stream<RedisRecord> sets = Stream.concat(
              records
                      .stream()
                      .filter(r -> r.value() != null)
                      .map(r -> RedisRecord.fromSetRecord(r, this.config)),
              records
                      .stream()
                      .map(r -> SinkOffsetState.of(r.topic(), r.kafkaPartition(), r.kafkaOffset()))
                      .map(RedisRecord::fromSinkOffsetState)
              );
      sets.forEach(s -> setOp.add(s.key(), s.value()));

      Arrays.asList(deleteOp, setOp).forEach(op -> {
        try {
          if (op.size() > 0) {
            log.debug(
                    "put() - Found  operation of type {} in {} record{s}. Executing operation...",
                    op.type,
                    op.size()
            );
            op.execute(this.session.asyncCommands());
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      });
    }

    private static String redisOffsetKey (TopicPartition topicPartition){
      return String.format("__kafka.offset.%s.%s", topicPartition.topic(), topicPartition.partition());
    }

    @Override
    public void stop () {
      try {
        if (null != this.session) {
          this.session.close();
        }
      } catch (Exception e) {
        log.warn("Exception thrown", e);
      }
    }
  }
