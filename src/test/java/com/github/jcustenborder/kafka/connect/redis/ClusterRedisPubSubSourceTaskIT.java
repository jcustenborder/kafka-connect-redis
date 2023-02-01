package com.github.jcustenborder.kafka.connect.redis;

import com.github.jcustenborder.docker.junit5.Compose;
import com.github.jcustenborder.kafka.connect.redis.healthchecks.RedisClusterHealthCheck;
import com.palantir.docker.compose.connection.Cluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Compose(
    dockerComposePath = "src/test/resources/docker/cluster/docker-compose.yml",
    clusterHealthCheck = RedisClusterHealthCheck.class
)
public class ClusterRedisPubSubSourceTaskIT extends AbstractTaskRedisPubSubSourceTaskIT {

  private static final Logger log = LoggerFactory.getLogger(ClusterRedisCacheSinkTaskIT.class);

  @Override
  protected ConnectionHelper createConnectionHelper(Cluster cluster) {
    return new ConnectionHelper.RedisCluster(cluster);
  }

}
