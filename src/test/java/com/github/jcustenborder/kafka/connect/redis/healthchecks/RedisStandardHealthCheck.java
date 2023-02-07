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
package com.github.jcustenborder.kafka.connect.redis.healthchecks;

import com.palantir.docker.compose.connection.Cluster;
import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.docker.compose.connection.waiting.SuccessOrFailure;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisStandardHealthCheck extends AbstractRedisHealthCheck {
  private static final Logger log = LoggerFactory.getLogger(RedisStandardHealthCheck.class);

  @Override
  public SuccessOrFailure isClusterHealthy(Cluster cluster) throws InterruptedException {
    return SuccessOrFailure.onResultOf(() -> {
      String ip = cluster.ip();
      Container container = cluster.container("redis");
      DockerPort dockerPort = container.port(6379);
      if (!dockerPort.isListeningNow()) {
        return false;
      }
      RedisURI redisURI = RedisURI.builder()
          .withHost(ip)
          .withPort(dockerPort.getExternalPort())
          .build();
      try (RedisClient client = RedisClient.create(redisURI)) {
        try (StatefulRedisConnection<String, String> connection = client.connect()) {
          RedisCommands<String, String> syncCommands = connection.sync();
          String ping = syncCommands.ping();
          log.info("ping = {}", ping);

          return testKeys(syncCommands);
        }
      }
    });
  }
}
