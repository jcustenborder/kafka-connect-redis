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
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class RedisSinkTask extends SinkTask {
  private static final Logger log = LoggerFactory.getLogger(RedisSinkTask.class);

  @Override
  public String version() {
    return VersionUtil.version(this.getClass());
  }

  RedisSinkConnectorConfig config;
  AbstractRedisClient client;
  StatefulConnection connection;
  RedisClusterAsyncCommands<byte[], byte[]> asyncCommands;

  @Override
  public void start(Map<String, String> settings) {
    this.config = new RedisSinkConnectorConfig(settings);
    this.client = RedisClusterClient.create(this.config.redisURIs());
    final RedisCodec<byte[], byte[]> codec = new ByteArrayCodec();
    if (RedisConnectorConfig.ClientMode.Cluster == this.config.clientMode) {
      final RedisClusterClient client = RedisClusterClient.create(this.config.redisURIs());
      final StatefulRedisClusterConnection<byte[], byte[]> connection = client.connect(codec);
      this.client = client;
      this.connection = connection;
      this.asyncCommands = connection.async();
    } else if (RedisConnectorConfig.ClientMode.Standalone == this.config.clientMode) {
      final RedisClient client = RedisClient.create(this.config.redisURIs().get(0));
      final StatefulRedisConnection<byte[], byte[]> connection = client.connect(codec);
      this.client = client;
      this.connection = connection;
      this.asyncCommands = connection.async();

    } else {
      throw new UnsupportedOperationException(
          String.format("%s is not supported", this.config.clientMode)
      );
    }
  }


  @Override
  public void put(Collection<SinkRecord> records) {
    log.debug("put() - Processing {} record(s)", records.size());
    List<SinkOperation> operations = new ArrayList<>(records.size());

    SinkOperation operation = SinkOperation.NONE;

    for (SinkRecord record : records) {
      SinkOperation.Type currentOperationType;

      if (null == record.value()) {
        currentOperationType = SinkOperation.Type.DELETE;
      } else {
        currentOperationType = SinkOperation.Type.SET;
      }

      if (currentOperationType != operation.type) {
        log.trace(
            "put() - Creating new operation. current={} last={}",
            currentOperationType,
            operation.type
        );
        operation = SinkOperation.create(currentOperationType, this.config, records.size());
        operations.add(operation);
      }
      operation.add(record);
    }

    log.debug(
        "put() - Found {} operation(s) in {} record{s}. Executing operations...",
        operations.size(),
        records.size()
    );

    for (SinkOperation op : operations) {
      log.debug("put() - Executing {} operation with {} values", op.type, op.size());
      try {
        op.execute(this.asyncCommands);
      } catch (InterruptedException e) {
        throw new RetriableException(e);
      }
    }
  }

  @Override
  public void stop() {
    this.connection.close();
  }
}
