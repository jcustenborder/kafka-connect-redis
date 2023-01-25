package com.github.jcustenborder.kafka.connect.redis;

import com.github.jcustenborder.docker.junit5.Compose;
import com.github.jcustenborder.docker.junit5.Port;
import org.junit.jupiter.api.BeforeEach;

import java.net.InetSocketAddress;

@Compose(
    dockerComposePath = "src/test/resources/docker/standard/docker-compose.yml"
)
public class StandardRedisPubSubSourceTaskIT extends AbstractRedisPubSubSourceTaskIT {
  @BeforeEach
  public void before(@Port(container = "redis", internalPort = 6379) InetSocketAddress address) {
    this.settings.put(RedisPubSubSourceConnectorConfig.HOSTS_CONFIG, String.format("%s:%s", address.getHostString(), address.getPort()));
    this.settings.put(RedisPubSubSourceConnectorConfig.CLIENT_MODE_CONFIG, RedisConnectorConfig.ClientMode.Standalone.name());
  }
}
