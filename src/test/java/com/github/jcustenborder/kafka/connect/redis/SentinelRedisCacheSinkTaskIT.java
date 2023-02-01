package com.github.jcustenborder.kafka.connect.redis;

import com.github.jcustenborder.docker.junit5.Compose;
import com.github.jcustenborder.kafka.connect.redis.healthchecks.RedisSentinelHealthCheck;
import com.palantir.docker.compose.connection.Cluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Compose(
    dockerComposePath = "src/test/resources/docker/sentinel/docker-compose.yml",
    clusterHealthCheck = RedisSentinelHealthCheck.class
)
public class SentinelRedisCacheSinkTaskIT extends AbstractTaskRedisCacheSinkTaskIT {
  private static final Logger log = LoggerFactory.getLogger(SentinelRedisCacheSinkTaskIT.class);

  @Override
  protected ConnectionHelper createConnectionHelper(Cluster cluster) {
    return new ConnectionHelper.Sentinel(cluster);
  }

  //  @BeforeEach
//  public void prepareSettings(
//      @DockerContainer(container = "redis") Container container
//  ) {
//    RedisConfigHelper.sentenielSettings(this.settings);
//  }
}
