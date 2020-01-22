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
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

class RedisSessionFactoryImpl implements RedisSessionFactory {
  Time time = Time.SYSTEM;

  private static final Logger log = LoggerFactory.getLogger(RedisSessionFactoryImpl.class);

  @Override
  public RedisSession create(RedisConnectorConfig config) {
    int attempts = 0;
    RedisSession result;

    while (true) {
      attempts++;
      try {
        log.info("Creating Redis session. Attempt {} of {}", attempts, config.maxAttempts);
        result = RedisSessionImpl.create(config);
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

  private static class RedisSessionImpl implements RedisSession {
    private static final Logger log = LoggerFactory.getLogger(RedisSessionImpl.class);

    private final AbstractRedisClient client;
    private final StatefulConnection connection;
    private final RedisClusterAsyncCommands<byte[], byte[]> asyncCommands;
    private final RedisConnectorConfig config;

    RedisSessionImpl(AbstractRedisClient client, StatefulConnection connection, RedisClusterAsyncCommands<byte[], byte[]> asyncCommands, RedisConnectorConfig config) {
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

    public static RedisSessionImpl create(RedisConnectorConfig config) {
      RedisSessionImpl result;
      final RedisCodec<byte[], byte[]> codec = new ByteArrayCodec();

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
        final RedisClusterClient client = RedisClusterClient.create(config.redisURIs());
        client.setOptions(clientOptions.build());

        final StatefulRedisClusterConnection<byte[], byte[]> connection = client.connect(codec);
        result = new RedisSessionImpl(client, connection, connection.async(), config);
      } else if (RedisConnectorConfig.ClientMode.Standalone == config.clientMode) {
        final ClientOptions.Builder clientOptions = ClientOptions.builder()
            .socketOptions(socketOptions)
            .requestQueueSize(config.requestQueueSize)
            .autoReconnect(config.autoReconnectEnabled);
        if (config.sslEnabled) {
          clientOptions.sslOptions(sslOptions);
        }
        final RedisClient client = RedisClient.create(config.redisURIs().get(0));
        client.setOptions(clientOptions.build());
        final StatefulRedisConnection<byte[], byte[]> connection = client.connect(codec);
        result = new RedisSessionImpl(client, connection, connection.async(), config);
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
}
