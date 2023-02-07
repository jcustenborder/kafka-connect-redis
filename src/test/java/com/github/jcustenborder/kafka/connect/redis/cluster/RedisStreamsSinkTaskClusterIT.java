package com.github.jcustenborder.kafka.connect.redis.cluster;

import com.github.jcustenborder.docker.junit5.Compose;
import com.github.jcustenborder.kafka.connect.redis.ConnectionHelper;
import com.github.jcustenborder.kafka.connect.redis.RedisStreamsSinkTaskIT;
import com.github.jcustenborder.kafka.connect.redis.healthchecks.RedisClusterHealthCheck;
import com.palantir.docker.compose.connection.Cluster;

@Compose(
    dockerComposePath = "src/test/resources/docker/cluster/docker-compose.yml",
    clusterHealthCheck = RedisClusterHealthCheck.class
)
public class RedisStreamsSinkTaskClusterIT extends RedisStreamsSinkTaskIT {
  @Override
  protected ConnectionHelper createConnectionHelper(Cluster cluster) {
    return new ConnectionHelper.RedisCluster(cluster);
  }
}
