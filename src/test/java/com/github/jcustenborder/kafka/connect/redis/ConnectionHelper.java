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

import com.palantir.docker.compose.connection.Cluster;
import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.DockerPort;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public abstract class ConnectionHelper {
  protected final Cluster cluster;

  ConnectionHelper(Cluster cluster) {
    this.cluster = cluster;
  }

  public abstract void appendSettings(Map<String, String> settings);


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
  }

  public static class RedisCluster extends ConnectionHelper {
    public RedisCluster(Cluster cluster) {
      super(cluster);
    }

    @Override
    public void appendSettings(Map<String, String> settings) {
      Container redisContainer = this.cluster.container("redis");
      List<String> hosts = new ArrayList<>();
      for (int port = 50000; port <= 50005; port++) {
        hosts.add(String.format("%s:%s", "127.0.0.1", port));
      }
      settings.put(RedisPubSubSourceConnectorConfig.HOSTS_CONFIG, String.join(",", hosts));
      settings.put(RedisPubSubSourceConnectorConfig.CLIENT_MODE_CONFIG, RedisConnectorConfig.ClientMode.Cluster.name());
    }
  }

  public static class Sentinel extends ConnectionHelper {
    public Sentinel(Cluster cluster) {
      super(cluster);
    }

    @Override
    public void appendSettings(Map<String, String> settings) {
      Container redisContainer = this.cluster.container("redis");
      List<String> hosts = new ArrayList<>();
      for (int port = 51000; port <= 51002; port++) {
        hosts.add(String.format("%s:%s", "127.0.0.1", port));
      }
      settings.put(RedisPubSubSourceConnectorConfig.HOSTS_CONFIG, String.join(",", hosts));
      settings.put(RedisPubSubSourceConnectorConfig.CLIENT_MODE_CONFIG, RedisConnectorConfig.ClientMode.Sentinel.name());
      settings.put(RedisPubSubSinkConnectorConfig.SENTINEL_MASTER_ID_CONFIG, "mymaster");
    }
  }
}