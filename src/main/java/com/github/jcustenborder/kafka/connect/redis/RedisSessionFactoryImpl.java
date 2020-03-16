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
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisConnectionException;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.SslOptions;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.pubsub.api.async.RedisPubSubAsyncCommands;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

class RedisSessionFactoryImpl implements RedisSessionFactory {
  private static final Logger log = LoggerFactory.getLogger(RedisSessionFactoryImpl.class);
  Time time = Time.SYSTEM;

  static <T extends AbstractRedisClient> T createClient(RedisConnectorConfig config, Class<T> cls) {
    T client;
    final SslOptions sslOptions;
    if (config.sslEnabled) {
      SslOptions.Builder builder = SslOptions.builder();
      switch (config.sslProvider) {
        case JDK:
          builder.jdkSslProvider();
          break;
        case OPENSSL:
          builder.openSslProvider();
          break;
        default:
          throw new UnsupportedOperationException(
              String.format(
                  "%s is not a supported value for %s.",
                  config.sslProvider,
                  RedisConnectorConfig.SSL_PROVIDER_CONFIG
              )
          );
      }
      if (null != config.keystorePath) {
        if (null != config.keystorePassword) {
          builder.keystore(config.keystorePath, config.keystorePassword.toCharArray());
        } else {
          builder.keystore(config.keystorePath);
        }
      }
      if (null != config.truststorePath) {
        if (null != config.truststorePassword) {
          builder.truststore(config.truststorePath, config.keystorePassword);
        } else {
          builder.truststore(config.truststorePath);
        }
      }
      sslOptions = builder.build();
    } else {
      sslOptions = null;
    }

    final SocketOptions socketOptions = SocketOptions.builder()
        .tcpNoDelay(config.tcpNoDelay)
        .connectTimeout(Duration.ofMillis(config.connectTimeout))
        .keepAlive(config.keepAliveEnabled)
        .build();

    if (RedisConnectorConfig.ClientMode.Cluster == config.clientMode) {
      ClusterClientOptions.Builder clientOptions = ClusterClientOptions.builder()
          .requestQueueSize(config.requestQueueSize)
          .autoReconnect(config.autoReconnectEnabled);
      if (config.sslEnabled) {
        clientOptions.sslOptions(sslOptions);
      }
      final RedisClusterClient clusterClient = RedisClusterClient.create(config.redisURIs());
      clusterClient.setOptions(clientOptions.build());
      client = (T) clusterClient;
    } else if (RedisConnectorConfig.ClientMode.Standalone == config.clientMode) {
      final ClientOptions.Builder clientOptions = ClientOptions.builder()
          .socketOptions(socketOptions)
          .requestQueueSize(config.requestQueueSize)
          .autoReconnect(config.autoReconnectEnabled);
      if (config.sslEnabled) {
        clientOptions.sslOptions(sslOptions);
      }
      final RedisClient standaloneClient = RedisClient.create(config.redisURIs().get(0));
      standaloneClient.setOptions(clientOptions.build());
      client = (T) standaloneClient;
    } else {
      throw new UnsupportedOperationException(
          String.format("%s is not supported", config.clientMode)
      );
    }

    return client;
  }

  @Override
  public RedisClusterSession<byte[], byte[]> createClusterSession(RedisConnectorConfig config) {
    final RedisCodec<byte[], byte[]> codec = new ByteArrayCodec();
    return createClusterSession(config, codec);
  }

  @Override
  public <K, V> RedisClusterSession<K, V> createClusterSession(RedisConnectorConfig config, RedisCodec<K, V> codec) {
    int attempts = 0;
    RedisClusterSession<K, V> result;

    while (true) {
      attempts++;
      try {
        log.info("Creating Redis session. Attempt {} of {}", attempts, config.maxAttempts);
        result = RedisClusterSessionImpl.create(config, codec);
        break;
      } catch (RedisConnectionException ex) {
        if (attempts == config.maxAttempts) {
          throw ex;
        } else {
          log.warn("Exception thrown connecting to redis. Waiting {} ms to try again.", config.retryDelay);
          this.time.sleep(config.retryDelay);
        }
      }
    }

    return result;
  }

  @Override
  public RedisPubSubSession<byte[], byte[]> createPubSubSession(RedisConnectorConfig config) {
    final RedisCodec<byte[], byte[]> codec = new ByteArrayCodec();
    return createPubSubSession(config, codec);
  }

  @Override
  public <K, V> RedisPubSubSession<K, V> createPubSubSession(RedisConnectorConfig config, RedisCodec<K, V> codec) {
    int attempts = 0;
    RedisPubSubSession<K, V> result;

    while (true) {
      attempts++;
      try {
        log.info("Creating Redis session. Attempt {} of {}", attempts, config.maxAttempts);
        result = RedisPubSubSessionImpl.create(config, codec);
        break;
      } catch (RedisConnectionException ex) {
        if (attempts == config.maxAttempts) {
          throw ex;
        } else {
          log.warn("Exception thrown connecting to redis. Waiting {} ms to try again.", config.retryDelay);
          this.time.sleep(config.retryDelay);
        }
      }
    }

    return result;
  }

  private static abstract class RedisSessionImpl<K, V, CONNECTION, COMMANDS> implements RedisSession<K, V, CONNECTION, COMMANDS> {
    protected final AbstractRedisClient client;
    protected final RedisConnectorConfig config;
    protected final CONNECTION connection;
    protected final COMMANDS commands;

    RedisSessionImpl(RedisConnectorConfig config,
                     AbstractRedisClient client,
                     CONNECTION connection,
                     COMMANDS commands
    ) {
      this.config = config;
      this.connection = connection;
      this.client = client;
      this.commands = commands;

    }

    @Override
    public COMMANDS asyncCommands() {
      return this.commands;
    }

    @Override
    public CONNECTION connection() {
      return this.connection;
    }

    @Override
    public AbstractRedisClient client() {
      return this.client;
    }
  }

  private static class RedisPubSubSessionImpl<K, V> extends RedisSessionImpl<K, V, StatefulRedisPubSubConnection<K, V>, RedisPubSubAsyncCommands<K, V>>
      implements RedisPubSubSession<K, V> {

    RedisPubSubSessionImpl(RedisConnectorConfig config, AbstractRedisClient client, StatefulRedisPubSubConnection<K, V> kvStatefulRedisPubSubConnection, RedisPubSubAsyncCommands<K, V> kvRedisPubSubAsyncCommands) {
      super(config, client, kvStatefulRedisPubSubConnection, kvRedisPubSubAsyncCommands);
    }

    static <K, V> RedisPubSubSession<K, V> create(RedisConnectorConfig config, RedisCodec<K, V> codec) {
      AbstractRedisClient client;
      StatefulRedisPubSubConnection<K, V> connection;
      RedisPubSubAsyncCommands<K, V> asyncCommands;

      if (RedisConnectorConfig.ClientMode.Cluster == config.clientMode) {
        final RedisClusterClient redisClusterClient = createClient(config, RedisClusterClient.class);
        final StatefulRedisPubSubConnection<K, V> clusterConnection = redisClusterClient.connectPubSub(codec);
        client = redisClusterClient;
        connection = clusterConnection;
        asyncCommands = clusterConnection.async();
      } else if (RedisConnectorConfig.ClientMode.Standalone == config.clientMode) {
        final RedisClient standaloneClient = createClient(config, RedisClient.class);
        final StatefulRedisPubSubConnection<K, V> standaloneConnection = standaloneClient.connectPubSub(codec);
        client = standaloneClient;
        connection = standaloneConnection;
        asyncCommands = standaloneConnection.async();
      } else {
        throw new UnsupportedOperationException(
            String.format("%s is not supported", config.clientMode)
        );
      }
      return new RedisPubSubSessionImpl<>(config, client, connection, asyncCommands);
    }

    @Override
    public void close() throws Exception {
      this.connection.close();
    }
  }

  private static class RedisClusterSessionImpl<K, V> extends RedisSessionImpl<K, V, StatefulConnection<K, V>, RedisClusterAsyncCommands<K, V>>
      implements RedisClusterSession<K, V> {
    private static final Logger log = LoggerFactory.getLogger(RedisClusterSessionImpl.class);

    RedisClusterSessionImpl(RedisConnectorConfig config, AbstractRedisClient client, StatefulConnection<K, V> connection, RedisClusterAsyncCommands<K, V> asyncCommands) {
      super(config, client, connection, asyncCommands);
    }

    static <K, V> RedisClusterSession<K, V> create(RedisConnectorConfig config, RedisCodec<K, V> codec) {
      AbstractRedisClient client;
      StatefulConnection<K, V> connection;
      RedisClusterAsyncCommands<K, V> asyncCommands;

      if (RedisConnectorConfig.ClientMode.Cluster == config.clientMode) {
        final RedisClusterClient redisClusterClient = createClient(config, RedisClusterClient.class);
        final StatefulRedisClusterConnection<K, V> clusterConnection = redisClusterClient.connect(codec);
        client = redisClusterClient;
        connection = clusterConnection;
        asyncCommands = clusterConnection.async();
      } else if (RedisConnectorConfig.ClientMode.Standalone == config.clientMode) {
        final RedisClient standaloneClient = createClient(config, RedisClient.class);
        final StatefulRedisConnection<K, V> standaloneConnection = standaloneClient.connect(codec);
        client = standaloneClient;
        connection = standaloneConnection;
        asyncCommands = standaloneConnection.async();
      } else {
        throw new UnsupportedOperationException(
            String.format("%s is not supported", config.clientMode)
        );
      }
      return new RedisClusterSessionImpl<>(config, client, connection, asyncCommands);
    }

    @Override
    public void close() throws Exception {
      this.connection.close();
      this.client.shutdown();
    }
  }
}
