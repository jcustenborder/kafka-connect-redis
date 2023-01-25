package com.github.jcustenborder.kafka.connect.redis;

import com.github.jcustenborder.docker.junit5.Compose;
import com.github.jcustenborder.docker.junit5.DockerContainer;
import com.github.jcustenborder.kafka.connect.redis.healthchecks.RedisSentinelHealthCheck;
import com.palantir.docker.compose.connection.Container;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Compose(
    dockerComposePath = "src/test/resources/docker/sentinel/docker-compose.yml",
    clusterHealthCheck = RedisSentinelHealthCheck.class
)
public class SentinelRedisCacheSinkTaskIT extends AbstractRedisCacheSinkTaskIT {
  private static final Logger log = LoggerFactory.getLogger(SentinelRedisCacheSinkTaskIT.class);

  @BeforeEach
  public void prepareSettings(
      @DockerContainer(container = "redis") Container container
  ) {
    log.info("container = {}", container);

    container.ports().stream().forEach(p -> {
      log.info("port: {} {}", p.getInternalPort(), p.getExternalPort());
    });
    RedisConfigHelper.sentenielSettings(this.settings);
//    String hosts = IntStream.range(48000, 48003).boxed()
//        .map(i -> String.format("127.0.0.1:%s", i))
//        .collect(Collectors.joining(","));
//
//    this.settings.put(RedisPubSubSourceConnectorConfig.HOSTS_CONFIG, hosts);
//    this.settings.put(RedisPubSubSourceConnectorConfig.CLIENT_MODE_CONFIG, RedisConnectorConfig.ClientMode.Sentinel.name());
//    this.settings.put(RedisPubSubSourceConnectorConfig.SENTINEL_MASTER_ID_CONFIG, "sentinel50000");
  }
}
