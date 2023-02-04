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
import com.palantir.docker.compose.connection.waiting.ClusterHealthCheck;
import com.palantir.docker.compose.connection.waiting.SuccessOrFailure;
import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.StringReader;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class RedisClusterHealthCheck implements ClusterHealthCheck {
  private static final Logger log = LoggerFactory.getLogger(RedisClusterHealthCheck.class);

  @Override
  public SuccessOrFailure isClusterHealthy(Cluster cluster) throws InterruptedException {
    return SuccessOrFailure.onResultOf(() -> {
      String ip = cluster.ip();
      List<RedisURI> redisURIS = IntStream.range(9000, 9005).boxed()
          .map(i -> RedisURI.create(ip, i))
          .collect(Collectors.toList());

      log.debug("Connecting to {}", redisURIS);
      try (RedisClusterClient clusterClient = RedisClusterClient.create(redisURIS)) {

        try (StatefulRedisClusterConnection<String, String> connection = clusterClient.connect()) {
          RedisAdvancedClusterCommands<String, String> syncCommands = connection.sync();

          Pattern pattern = Pattern.compile("(.+):(.+)");
          String clusterInfo = syncCommands.clusterInfo();
          log.trace("Cluster info:\n{}", clusterInfo);
          try (StringReader reader = new StringReader(clusterInfo)) {
            try (BufferedReader bufferedReader = new BufferedReader(reader)) {
              String line;
              while ((line = bufferedReader.readLine()) != null) {
                Matcher matcher = pattern.matcher(line);
                if (matcher.find()) {
                  String key = matcher.group(1);
                  String value = matcher.group(2);

                  if ("cluster_state".equals(key) && "ok".equals(value)) {
                    return true;
                  }
                }
              }
            }
          }
          return false;
        }
      }
    });
  }
}
