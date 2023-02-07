package com.github.jcustenborder.kafka.connect.redis.sentinel;

import com.github.jcustenborder.docker.junit5.Compose;
import com.github.jcustenborder.kafka.connect.redis.ConnectionHelper;
import com.github.jcustenborder.kafka.connect.redis.RedisStreamsSinkTaskIT;
import com.github.jcustenborder.kafka.connect.redis.healthchecks.RedisSentinelHealthCheck;
import com.palantir.docker.compose.connection.Cluster;

@Compose(
    dockerComposePath = "src/test/resources/docker/sentinel/docker-compose.yml",
    clusterHealthCheck = RedisSentinelHealthCheck.class
)
public class RedisStreamsSinkTaskSentinelIT extends RedisStreamsSinkTaskIT {
  @Override
  protected ConnectionHelper createConnectionHelper(Cluster cluster) {
    return new ConnectionHelper.Sentinel(cluster);
  }
}
