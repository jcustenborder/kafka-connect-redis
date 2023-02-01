package com.github.jcustenborder.kafka.connect.redis;

import com.palantir.docker.compose.connection.Cluster;
import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.DockerPort;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;

import java.util.Map;

abstract class ConnectionHelper {
  protected final Cluster cluster;

  ConnectionHelper(Cluster cluster) {
    this.cluster = cluster;
  }

  public abstract void appendSettings(Map<String, String> settings);

  public abstract StatefulRedisConnection<String, String> redisConnection();

  public static final class Standard extends ConnectionHelper {
    public Standard(Cluster cluster) {
      super(cluster);
    }

    @Override
    public void appendSettings(Map<String, String> settings) {
      Container redisContainer = this.cluster.container("redis");
      DockerPort redisPort = redisContainer.port(6379);
      settings.put(RedisPubSubSourceConnectorConfig.HOSTS_CONFIG, String.format("%s:%s", redisPort.getIp(), redisPort.getExternalPort()));
      settings.put(RedisPubSubSourceConnectorConfig.CLIENT_MODE_CONFIG, RedisConnectorConfig.ClientMode.Standalone.name());
    }

    @Override
    public StatefulRedisConnection redisConnection() {
      Container redisContainer = this.cluster.container("redis");
      DockerPort redisPort = redisContainer.port(6379);
      RedisURI redisURI = RedisURI.builder()
          .withHost(redisPort.getIp())
          .withPort(redisPort.getExternalPort())
          .withDatabase(1)
          .build();

      RedisClient client = RedisClient.create(redisURI);
      return client.connect();
    }
  }

  public static class RedisCluster extends ConnectionHelper {
    public RedisCluster(Cluster cluster) {
      super(cluster);
    }

    @Override
    public void appendSettings(Map<String, String> settings) {
      throw new UnsupportedOperationException();
    }

    @Override
    public StatefulRedisConnection redisConnection() {
      throw new UnsupportedOperationException();
    }
  }

  public static class Sentinel extends ConnectionHelper {
    public Sentinel(Cluster cluster) {
      super(cluster);
    }

    @Override
    public void appendSettings(Map<String, String> settings) {
      throw new UnsupportedOperationException();
    }

    @Override
    public StatefulRedisConnection redisConnection() {
      throw new UnsupportedOperationException();
    }
  }
}
