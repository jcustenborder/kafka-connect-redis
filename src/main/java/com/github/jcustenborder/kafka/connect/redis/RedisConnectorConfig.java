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

import com.github.jcustenborder.kafka.connect.utils.config.ConfigKeyBuilder;
import com.github.jcustenborder.kafka.connect.utils.config.ConfigUtils;
import com.github.jcustenborder.kafka.connect.utils.config.ValidEnum;
import com.google.common.base.Strings;
import com.google.common.net.HostAndPort;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SocketOptions;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

class RedisConnectorConfig extends AbstractConfig {
  private static final Logger log = LoggerFactory.getLogger(RedisConnectorConfig.class);
  public static final String HOSTS_CONFIG = "redis.hosts";
  static final String HOSTS_DOC = "The Redis hosts to connect to.";
  public static final String SSL_CONFIG = "redis.ssl.enabled";
  static final String SSL_DOC = "Flag to determine if SSL is enabled.";
  public static final String PASSWORD_CONFIG = "redis.password";
  static final String PASSWORD_DOC = "Password used to connect to Redis.";
  public static final String DATABASE_CONFIG = "redis.database";
  static final String DATABASE_DOC = "Redis database to connect to.";
  public static final String CLIENT_MODE_CONFIG = "redis.client.mode";
  static final String CLIENT_MODE_DOC = "The client mode to use when interacting with the Redis " +
      "cluster.";
  public static final String AUTO_RECONNECT_ENABLED_CONFIG = "redis.auto.reconnect.enabled";
  static final String AUTO_RECONNECT_ENABLED_DOC = "Flag to determine if the Redis client should " +
      "automatically reconnect.";
  public static final String REQUEST_QUEUE_SIZE_CONFIG = "redis.request.queue.size";
  static final String REQUEST_QUEUE_SIZE_DOC = "The maximum number of queued requests to Redis.";
  public static final String SOCKET_TCP_NO_DELAY_CONFIG = "redis.socket.tcp.no.delay.enabled";
  static final String SOCKET_TCP_NO_DELAY_DOC = "Flag to enable TCP no delay should be used.";
  public static final String SOCKET_KEEP_ALIVE_CONFIG = "redis.socket.keep.alive.enabled";
  static final String SOCKET_KEEP_ALIVE_DOC = "Flag to enable a keepalive to Redis.";
  public static final String SOCKET_CONNECT_TIMEOUT_CONFIG = "redis.socket.connect.timeout.ms";
  static final String SOCKET_CONNECT_TIMEOUT_DOC = "The amount of time in milliseconds to wait " +
      "before timing out a socket when connecting.";
  public static final String SSL_PROVIDER_CONFIG = "redis.ssl.provider";
  static final String SSL_PROVIDER_DOC = "The SSL provider to use.";
  public static final String SSL_KEYSTORE_PATH_CONFIG = "redis.ssl.keystore.path";
  static final String SSL_KEYSTORE_PATH_DOC = "The path to the SSL keystore.";
  public static final String SSL_KEYSTORE_PASSWORD_CONFIG = "redis.ssl.keystore.password";
  static final String SSL_KEYSTORE_PASSWORD_DOC = "The password for the SSL keystore.";
  public static final String SSL_TRUSTSTORE_PATH_CONFIG = "redis.ssl.truststore.path";
  static final String SSL_TRUSTSTORE_PATH_DOC = "The path to the SSL truststore.";
  public static final String SSL_TRUSTSTORE_PASSWORD_CONFIG = "redis.ssl.truststore.password";
  static final String SSL_TRUSTSTORE_PASSWORD_DOC = "The password for the SSL truststore.";
  
  public final ClientMode clientMode;
  public final List<HostAndPort> hosts;

  public final String password;
  public final int database;
  public final boolean autoReconnectEnabled;
  public final int requestQueueSize;

  public final boolean tcpNoDelay;
  public final boolean keepAliveEnabled;
  public final int connectTimeout;

  public final boolean sslEnabled;
  public final RedisSslProvider sslProvider;
  public final File keystorePath;
  public final String keystorePassword;
  public final File truststorePath;
  public final String truststorePassword;



  public RedisConnectorConfig(ConfigDef config, Map<?, ?> originals) {
    super(config, originals);
    this.hosts = ConfigUtils.hostAndPorts(this, HOSTS_CONFIG, 6379);
    this.sslEnabled = getBoolean(SSL_CONFIG);
    this.password = getPassword(PASSWORD_CONFIG).value();
    this.database = getInt(DATABASE_CONFIG);
    this.clientMode = ConfigUtils.getEnum(ClientMode.class, this, CLIENT_MODE_CONFIG);
    this.autoReconnectEnabled = getBoolean(AUTO_RECONNECT_ENABLED_CONFIG);
    this.requestQueueSize = getInt(REQUEST_QUEUE_SIZE_CONFIG);
    this.keepAliveEnabled = getBoolean(SOCKET_KEEP_ALIVE_CONFIG);
    this.tcpNoDelay = getBoolean(SOCKET_TCP_NO_DELAY_CONFIG);
    this.connectTimeout = getInt(SOCKET_CONNECT_TIMEOUT_CONFIG);
    this.sslProvider = ConfigUtils.getEnum(RedisSslProvider.class, this, SSL_PROVIDER_CONFIG);
    final String keystorePath = getString(SSL_KEYSTORE_PATH_CONFIG);
    final String trustStorePath = getString(SSL_TRUSTSTORE_PATH_CONFIG);
    this.keystorePath = Strings.isNullOrEmpty(keystorePath) ? null : new File(keystorePath);
    this.truststorePath = Strings.isNullOrEmpty(trustStorePath) ? null : new File(trustStorePath);
    final String keystorePassword = getPassword(SSL_KEYSTORE_PASSWORD_CONFIG).value();
    final String trustPassword = getPassword(SSL_TRUSTSTORE_PASSWORD_CONFIG).value();
    this.keystorePassword = Strings.isNullOrEmpty(keystorePassword) ? null : keystorePassword;
    this.truststorePassword = Strings.isNullOrEmpty(trustPassword) ? null : trustPassword;
  }

  public static ConfigDef config() {
    return new ConfigDef()
        .define(
            ConfigKeyBuilder.of(HOSTS_CONFIG, ConfigDef.Type.LIST)
                .documentation(HOSTS_DOC)
                .defaultValue(Arrays.asList("localhost:6379"))
                .importance(ConfigDef.Importance.HIGH)
                .build()
        ).define(
            ConfigKeyBuilder.of(CLIENT_MODE_CONFIG, ConfigDef.Type.STRING)
                .documentation(CLIENT_MODE_DOC)
                .defaultValue(ClientMode.Standalone.toString())
                .validator(ValidEnum.of(ClientMode.class))
                .importance(ConfigDef.Importance.MEDIUM)
                .build()
        ).define(
            ConfigKeyBuilder.of(SSL_CONFIG, ConfigDef.Type.BOOLEAN)
                .documentation(SSL_DOC)
                .defaultValue(false)
                .importance(ConfigDef.Importance.MEDIUM)
                .build()
        ).define(
            ConfigKeyBuilder.of(PASSWORD_CONFIG, ConfigDef.Type.PASSWORD)
                .documentation(PASSWORD_DOC)
                .defaultValue("")
                .importance(ConfigDef.Importance.MEDIUM)
                .build()
        ).define(
            ConfigKeyBuilder.of(DATABASE_CONFIG, ConfigDef.Type.INT)
                .documentation(DATABASE_DOC)
                .defaultValue(1)
                .importance(ConfigDef.Importance.MEDIUM)
                .build()
        ).define(
            ConfigKeyBuilder.of(AUTO_RECONNECT_ENABLED_CONFIG, ConfigDef.Type.BOOLEAN)
                .documentation(AUTO_RECONNECT_ENABLED_DOC)
                .defaultValue(ClientOptions.DEFAULT_AUTO_RECONNECT)
                .importance(ConfigDef.Importance.LOW)
                .build()
        ).define(
            ConfigKeyBuilder.of(REQUEST_QUEUE_SIZE_CONFIG, ConfigDef.Type.INT)
                .documentation(REQUEST_QUEUE_SIZE_DOC)
                .defaultValue(ClientOptions.DEFAULT_REQUEST_QUEUE_SIZE)
                .importance(ConfigDef.Importance.LOW)
                .build()
        ).define(
            ConfigKeyBuilder.of(SOCKET_TCP_NO_DELAY_CONFIG, ConfigDef.Type.BOOLEAN)
                .documentation(SOCKET_TCP_NO_DELAY_DOC)
                .defaultValue(true)
                .importance(ConfigDef.Importance.LOW)
                .build()
        ).define(
            ConfigKeyBuilder.of(SOCKET_KEEP_ALIVE_CONFIG, ConfigDef.Type.BOOLEAN)
                .documentation(SOCKET_KEEP_ALIVE_DOC)
                .defaultValue(SocketOptions.DEFAULT_SO_KEEPALIVE)
                .importance(ConfigDef.Importance.LOW)
                .build()
        ).define(
            ConfigKeyBuilder.of(SOCKET_CONNECT_TIMEOUT_CONFIG, ConfigDef.Type.INT)
                .documentation(SOCKET_CONNECT_TIMEOUT_DOC)
                .defaultValue((int) SocketOptions.DEFAULT_CONNECT_TIMEOUT_DURATION.toMillis())
                .importance(ConfigDef.Importance.LOW)
                .build()
        ).define(
            ConfigKeyBuilder.of(SSL_PROVIDER_CONFIG, ConfigDef.Type.STRING)
                .documentation(SSL_PROVIDER_DOC)
                .defaultValue(RedisSslProvider.JDK.toString())
                .importance(ConfigDef.Importance.LOW)
                .validator(ValidEnum.of(RedisSslProvider.class))
                .build()
        ).define(
            ConfigKeyBuilder.of(SSL_KEYSTORE_PATH_CONFIG, ConfigDef.Type.STRING)
                .documentation(SSL_KEYSTORE_PATH_DOC)
                .defaultValue("")
                .importance(ConfigDef.Importance.MEDIUM)
                .build()
        ).define(
            ConfigKeyBuilder.of(SSL_KEYSTORE_PASSWORD_CONFIG, ConfigDef.Type.PASSWORD)
                .documentation(SSL_KEYSTORE_PASSWORD_DOC)
                .defaultValue("")
                .importance(ConfigDef.Importance.MEDIUM)
                .build()
        ).define(
            ConfigKeyBuilder.of(SSL_TRUSTSTORE_PATH_CONFIG, ConfigDef.Type.STRING)
                .documentation(SSL_TRUSTSTORE_PATH_DOC)
                .defaultValue("")
                .importance(ConfigDef.Importance.MEDIUM)
                .build()
        ).define(
            ConfigKeyBuilder.of(SSL_TRUSTSTORE_PASSWORD_CONFIG, ConfigDef.Type.PASSWORD)
                .documentation(SSL_TRUSTSTORE_PASSWORD_DOC)
                .defaultValue("")
                .importance(ConfigDef.Importance.MEDIUM)
                .build()
        );
  }

  public List<RedisURI> redisURIs() {
    List<RedisURI> result = new ArrayList<>();

    for (HostAndPort host : this.hosts) {
      RedisURI.Builder builder = RedisURI.builder();
      builder.withHost(host.getHost());
      builder.withPort(host.getPort());
      if (!Strings.isNullOrEmpty(this.password)) {
        builder.withPassword(this.password);
      }
      builder.withSsl(this.sslEnabled);
      result.add(builder.build());
    }

    return result;
  }

  public enum ClientMode {
    Standalone,
    Cluster
  }

  public enum RedisSslProvider {
    OPENSSL,
    JDK
  }
}
