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
public class ClusterRedisCacheSinkTaskIT extends AbstractTaskRedisCacheSinkTaskIT {
  private static final Logger log = LoggerFactory.getLogger(ClusterRedisCacheSinkTaskIT.class);

  @Override
  protected ConnectionHelper createConnectionHelper(Cluster cluster) {
    return new ConnectionHelper.RedisCluster(cluster);
  }

  //  @BeforeEach
//  public void prepareSettings(
//      @DockerContainer(container = "redis") Container container
//  ) {
//    log.info("container = {}", container);
//
//    container.ports().stream().forEach(p -> {
//      log.info("port: {} {}", p.getInternalPort(), p.getExternalPort());
//    });
//
//    String hosts = IntStream.range(9000, 9005).boxed()
//        .map(i -> String.format("127.0.0.1:%s", i))
//        .collect(Collectors.joining(","));
//
//    this.settings.put(RedisPubSubSourceConnectorConfig.HOSTS_CONFIG, hosts);
//    this.settings.put(RedisPubSubSourceConnectorConfig.CLIENT_MODE_CONFIG, RedisConnectorConfig.ClientMode.Cluster.name());
//  }
}
