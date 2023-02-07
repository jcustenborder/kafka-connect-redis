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
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.async.RedisGeoAsyncCommands;
import io.lettuce.core.api.async.RedisHashAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import io.lettuce.core.api.async.RedisSetAsyncCommands;
import io.lettuce.core.api.async.RedisSortedSetAsyncCommands;
import io.lettuce.core.api.async.RedisStreamAsyncCommands;
import io.lettuce.core.api.async.RedisStringAsyncCommands;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.lettuce.core.cluster.pubsub.RedisClusterPubSubListener;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.masterreplica.MasterReplica;
import io.lettuce.core.pubsub.RedisPubSubListener;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.pubsub.api.async.RedisPubSubAsyncCommands;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

class RedisSessionFactoryImpl implements RedisSessionFactory {
  private static final Logger log = LoggerFactory.getLogger(RedisSessionFactoryImpl.class);
  Time time = Time.SYSTEM;


  static void setCommonOptions(RedisConnectorConfig config, ClientOptions.Builder clientOptionsBuilder) {
    if (config.sslEnabled) {
      final SslOptions sslOptions;
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
      clientOptionsBuilder.sslOptions(sslOptions);
    }

    final SocketOptions socketOptions = SocketOptions.builder()
        .tcpNoDelay(config.tcpNoDelay)
        .connectTimeout(Duration.ofMillis(config.connectTimeout))
        .keepAlive(config.keepAliveEnabled)
        .build();

    clientOptionsBuilder.autoReconnect(config.autoReconnectEnabled)
        .requestQueueSize(config.requestQueueSize)
        .socketOptions(socketOptions);

  }

  static <T extends ClientOptions> T createClientOptions(RedisConnectorConfig config, Class<T> cls) {
    if (RedisConnectorConfig.ClientMode.Cluster == config.clientMode) {
      ClusterClientOptions.Builder clusterClientOptionsBuilder = ClusterClientOptions.builder();

      setCommonOptions(config, clusterClientOptionsBuilder);
      return (T) clusterClientOptionsBuilder.build();
    } else if (RedisConnectorConfig.ClientMode.Standalone == config.clientMode) {
      ClientOptions.Builder clientOptionsBuilder = ClientOptions.builder();
      setCommonOptions(config, clientOptionsBuilder);
      return (T) clientOptionsBuilder.build();
    } else if (RedisConnectorConfig.ClientMode.Sentinel == config.clientMode) {
      ClusterClientOptions.Builder clientOptionsBuilder = ClusterClientOptions.builder();
      clientOptionsBuilder.topologyRefreshOptions(
          ClusterTopologyRefreshOptions.builder()
              .enableAllAdaptiveRefreshTriggers()
              .enablePeriodicRefresh(true)
              .dynamicRefreshSources(true)
              .refreshPeriod(Duration.ofSeconds(30))
              .build()
      );
      setCommonOptions(config, clientOptionsBuilder);
      return (T) clientOptionsBuilder.build();
    } else {
      throw new UnsupportedOperationException(
          String.format("%s is not supported", config.clientMode)
      );
    }
  }

  static <T extends AbstractRedisClient> T createClient(RedisConnectorConfig config, Class<T> cls) {
    T client;

    if (RedisConnectorConfig.ClientMode.Cluster == config.clientMode) {
      final ClusterClientOptions clientOptions = createClientOptions(config, ClusterClientOptions.class);
      final RedisClusterClient clusterClient = RedisClusterClient.create(config.redisURIs());
      clusterClient.setOptions(clientOptions);
      client = (T) clusterClient;
    } else if (RedisConnectorConfig.ClientMode.Standalone == config.clientMode) {
      final ClientOptions clientOptions = createClientOptions(config, ClientOptions.class);
      final RedisClient standaloneClient = RedisClient.create(config.redisURIs().get(0));
      standaloneClient.setOptions(clientOptions);
      client = (T) standaloneClient;
    } else if (RedisConnectorConfig.ClientMode.Sentinel == config.clientMode) {
      final ClusterClientOptions clientOptions = createClientOptions(config, ClusterClientOptions.class);
      final RedisClient sentinelClient = RedisClient.create(config.redisURIs().get(0));
      sentinelClient.setOptions(clientOptions);
      client = (T) sentinelClient;
    } else {
      throw new UnsupportedOperationException(
          String.format("%s is not supported", config.clientMode)
      );
    }

    return client;
  }

  @Override
  public RedisSession createSession(RedisConnectorConfig config) {
    int attempts = 0;
    RedisSession result;

    while (true) {
      attempts++;
      try {
        log.info("Creating Redis session. Attempt {} of {}", attempts, config.maxAttempts);
        result = create(config);
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
  public RedisPubSubSession createPubSubSession(RedisConnectorConfig config) {
    int attempts = 0;
    RedisPubSubSession result;

    while (true) {
      attempts++;
      try {
        log.info("Creating Redis session. Attempt {} of {}", attempts, config.maxAttempts);
        result = createPubSub(config);
        break;
      } catch (RedisConnectionException ex) {
        if (attempts == config.maxAttempts) {
          throw ex;
        } else {
          log.warn("Exception thrown connecting to redis. Waiting {} ms to try again.", config.retryDelay, ex);
          this.time.sleep(config.retryDelay);
        }
      }
    }

    return result;
  }


  static RedisPubSubSession createPubSub(RedisConnectorConfig config) {
    final RedisCodec<byte[], byte[]> codec = ByteArrayCodec.INSTANCE;
    final RedisPubSubSession result;

    if (RedisConnectorConfig.ClientMode.Cluster == config.clientMode) {
      final RedisClusterClient redisClusterClient = createClient(config, RedisClusterClient.class);
      final StatefulRedisClusterPubSubConnection<byte[], byte[]> clusterConnection = redisClusterClient.connectPubSub(codec);
      result = new ClusterPubSubSessionImpl(clusterConnection, redisClusterClient);
    } else if (RedisConnectorConfig.ClientMode.Standalone == config.clientMode) {
      final RedisClient standaloneClient = createClient(config, RedisClient.class);
      final StatefulRedisPubSubConnection<byte[], byte[]> standaloneConnection = standaloneClient.connectPubSub(codec);
      result = new PubSubSessionImpl(standaloneConnection, standaloneClient);
    } else if (RedisConnectorConfig.ClientMode.Sentinel == config.clientMode) {
      final RedisClient standaloneClient = createClient(config, RedisClient.class);
      final StatefulRedisPubSubConnection<byte[], byte[]> standaloneConnection = standaloneClient.connectPubSub(codec);
      result = new PubSubSessionImpl(standaloneConnection, standaloneClient);
    } else {
      throw new UnsupportedOperationException(
          String.format("%s is not supported", config.clientMode)
      );
    }
    return result;
  }

  static RedisSession create(RedisConnectorConfig config) {
    final RedisSession result;
    final RedisCodec<byte[], byte[]> codec = ByteArrayCodec.INSTANCE;

    if (RedisConnectorConfig.ClientMode.Standalone == config.clientMode ||
        RedisConnectorConfig.ClientMode.Sentinel == config.clientMode) {
      final StatefulRedisConnection<byte[], byte[]> connection;
      final RedisClient client;
      if (RedisConnectorConfig.ClientMode.Standalone == config.clientMode) {
        client = createClient(config, RedisClient.class);
        connection = client.connect(codec);
      } else if (RedisConnectorConfig.ClientMode.Sentinel == config.clientMode) {
        client = createClient(config, RedisClient.class);
        connection = MasterReplica.connect(client, codec, config.redisURIs().get(0));
      } else {
        throw new UnsupportedOperationException(
            String.format("%s is not supported", config.clientMode)
        );
      }
      result = new RedisSessionImpl(connection, client);
    } else {
      final RedisClusterClient redisClusterClient = createClient(config, RedisClusterClient.class);
      final StatefulRedisClusterConnection<byte[], byte[]> connection = redisClusterClient.connect(codec);
      result = new RedisClusterSessionImpl(connection, redisClusterClient);
    }
    return result;
  }

  static abstract class AbstractRedisSession<CONN extends StatefulConnection<byte[], byte[]>> implements RedisSession {
    protected final CONN connection;
    protected final AbstractRedisClient client;

    public AbstractRedisSession(CONN connection, AbstractRedisClient client) {
      this.connection = connection;
      this.client = client;
    }

    @Override
    public void flushCommands() {
      this.connection.flushCommands();
    }

    @Override
    public void setAutoFlushCommands(boolean enabled) {
      this.connection.setAutoFlushCommands(enabled);
    }

    @Override
    public void close() throws Exception {
      this.connection.close();
      this.client.shutdown();
    }
  }

  static class RedisSessionImpl extends AbstractRedisSession<StatefulRedisConnection<byte[], byte[]>> {
    private final RedisAsyncCommands<byte[], byte[]> asyncCommands;

    RedisSessionImpl(StatefulRedisConnection<byte[], byte[]> connection, AbstractRedisClient client) {
      super(connection, client);
      this.asyncCommands = this.connection.async();
    }

    @Override
    public RedisSetAsyncCommands<byte[], byte[]> set() {
      return this.asyncCommands;
    }

    @Override
    public RedisGeoAsyncCommands<byte[], byte[]> geo() {
      return this.asyncCommands;
    }

    @Override
    public RedisHashAsyncCommands<byte[], byte[]> hash() {
      return this.asyncCommands;
    }

    @Override
    public RedisStringAsyncCommands<byte[], byte[]> string() {
      return this.asyncCommands;
    }

    @Override
    public RedisKeyAsyncCommands<byte[], byte[]> key() {
      return this.asyncCommands;
    }

    @Override
    public RedisSortedSetAsyncCommands<byte[], byte[]> sortedSet() {
      return this.asyncCommands;
    }

    @Override
    public RedisStreamAsyncCommands<byte[], byte[]> streams() {
      return this.asyncCommands;
    }
  }


  static class RedisClusterSessionImpl extends AbstractRedisSession<StatefulRedisClusterConnection<byte[], byte[]>> {
    private final RedisClusterAsyncCommands<byte[], byte[]> asyncCommands;

    RedisClusterSessionImpl(StatefulRedisClusterConnection<byte[], byte[]> connection, AbstractRedisClient client) {
      super(connection, client);
      this.asyncCommands = this.connection.async();
    }

    @Override
    public RedisSetAsyncCommands<byte[], byte[]> set() {
      return this.asyncCommands;
    }

    @Override
    public RedisGeoAsyncCommands<byte[], byte[]> geo() {
      return this.asyncCommands;
    }

    @Override
    public RedisHashAsyncCommands<byte[], byte[]> hash() {
      return this.asyncCommands;
    }

    @Override
    public RedisStringAsyncCommands<byte[], byte[]> string() {
      return this.asyncCommands;
    }

    @Override
    public RedisKeyAsyncCommands<byte[], byte[]> key() {
      return this.asyncCommands;
    }

    @Override
    public RedisSortedSetAsyncCommands<byte[], byte[]> sortedSet() {
      return this.asyncCommands;
    }

    @Override
    public RedisStreamAsyncCommands<byte[], byte[]> streams() {
      return this.asyncCommands;
    }
  }

  static abstract class AbstractPubSubSession<CONNECTION extends StatefulRedisPubSubConnection<byte[], byte[]>> implements RedisPubSubSession {
    protected final CONNECTION connection;
    protected final AbstractRedisClient client;
    protected final RedisPubSubAsyncCommands<byte[], byte[]> asyncCommands;

    AbstractPubSubSession(CONNECTION connection, AbstractRedisClient client) {
      this.connection = connection;
      this.client = client;
      this.asyncCommands = this.connection.async();
    }


    @Override
    public RedisPubSubAsyncCommands<byte[], byte[]> asyncCommands() {
      return this.asyncCommands;
    }

    @Override
    public void close() throws Exception {
      this.connection.close();
      this.client.shutdown();
    }
  }

  static class ClusterPubSubSessionImpl extends AbstractPubSubSession<StatefulRedisClusterPubSubConnection<byte[], byte[]>> {
    ClusterPubSubSessionImpl(StatefulRedisClusterPubSubConnection<byte[], byte[]> connection, AbstractRedisClient client) {
      super(connection, client);
    }

    @Override
    public void addPubSubListener(RedisPubSubListener<byte[], byte[]> listener) {
      this.connection.addListener(listener);
    }

    @Override
    public void addClusterPubSubListener(RedisClusterPubSubListener<byte[], byte[]> listener) {
      this.connection.addListener(listener);
    }

    @Override
    public void close() throws Exception {
      this.connection.close();
    }
  }

  static class PubSubSessionImpl extends AbstractPubSubSession<StatefulRedisPubSubConnection<byte[], byte[]>> {
    PubSubSessionImpl(StatefulRedisPubSubConnection<byte[], byte[]> connection, AbstractRedisClient client) {
      super(connection, client);
    }

    @Override
    public void addPubSubListener(RedisPubSubListener<byte[], byte[]> listener) {
      this.connection.addListener(listener);
    }

    @Override
    public void addClusterPubSubListener(RedisClusterPubSubListener<byte[], byte[]> listener) {

    }

    @Override
    public void close() throws Exception {
      this.connection.close();
    }
  }

}
