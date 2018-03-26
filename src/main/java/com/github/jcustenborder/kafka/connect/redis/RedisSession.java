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

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RedisSession implements AutoCloseable {
  private static final Logger log = LoggerFactory.getLogger(RedisSession.class);

  private final AbstractRedisClient client;
  private final StatefulConnection connection;
  private final RedisClusterAsyncCommands<byte[], byte[]> asyncCommands;
  private final RedisSinkConnectorConfig config;

  RedisSession(AbstractRedisClient client, StatefulConnection connection, RedisClusterAsyncCommands<byte[], byte[]> asyncCommands, RedisSinkConnectorConfig config) {
    this.client = client;
    this.connection = connection;
    this.asyncCommands = asyncCommands;
    this.config = config;
  }

  public AbstractRedisClient client() {
    return this.client;
  }

  public StatefulConnection connection() {
    return this.connection;
  }

  public RedisClusterAsyncCommands<byte[], byte[]> asyncCommands() {
    return this.asyncCommands;
  }

  public static RedisSession create(RedisSinkConnectorConfig config) {
    RedisSession result;
    final RedisCodec<byte[], byte[]> codec = new ByteArrayCodec();

    if (RedisConnectorConfig.ClientMode.Cluster == config.clientMode) {
      final RedisClusterClient client = RedisClusterClient.create(config.redisURIs());
      final StatefulRedisClusterConnection<byte[], byte[]> connection = client.connect(codec);
      result = new RedisSession(client, connection, connection.async(), config);
    } else if (RedisConnectorConfig.ClientMode.Standalone == config.clientMode) {
      final RedisClient client = RedisClient.create(config.redisURIs().get(0));
      final StatefulRedisConnection<byte[], byte[]> connection = client.connect(codec);
      result = new RedisSession(client, connection, connection.async(), config);
    } else {
      throw new UnsupportedOperationException(
          String.format("%s is not supported", config.clientMode)
      );
    }

    return result;
  }


  @Override
  public void close() throws Exception {
    this.connection.close();
  }
}
