package com.github.jcustenborder.kafka.connect.redis.healthchecks;

import com.palantir.docker.compose.connection.Cluster;
import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.docker.compose.connection.waiting.ClusterHealthCheck;
import com.palantir.docker.compose.connection.waiting.SuccessOrFailure;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisStandardHealthCheck implements ClusterHealthCheck {
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


          return true;
        }
      }
    });
  }
}
