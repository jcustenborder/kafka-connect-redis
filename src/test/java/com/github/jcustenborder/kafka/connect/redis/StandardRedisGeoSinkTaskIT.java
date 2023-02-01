package com.github.jcustenborder.kafka.connect.redis;

import com.github.jcustenborder.docker.junit5.Compose;
import com.github.jcustenborder.kafka.connect.redis.healthchecks.RedisStandardHealthCheck;
import com.palantir.docker.compose.connection.Cluster;

@Compose(
    dockerComposePath = "src/test/resources/docker/standard/docker-compose.yml",
    clusterHealthCheck = RedisStandardHealthCheck.class
)
public class StandardRedisGeoSinkTaskIT extends AbstractTaskRedisGeoSinkTaskIT {
//  @BeforeEach
//  public void before(@Port(container = "redis", internalPort = 6379) InetSocketAddress address) {
//    this.settings.put(RedisPubSubSourceConnectorConfig.HOSTS_CONFIG, String.format("%s:%s", address.getHostString(), address.getPort()));
//    this.settings.put(RedisPubSubSourceConnectorConfig.CLIENT_MODE_CONFIG, RedisConnectorConfig.ClientMode.Standalone.name());
//  }

  @Override
  protected ConnectionHelper createConnectionHelper(Cluster cluster) {
    return new ConnectionHelper.Standard(cluster);
  }
}
