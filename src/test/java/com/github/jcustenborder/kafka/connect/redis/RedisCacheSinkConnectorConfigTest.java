package com.github.jcustenborder.kafka.connect.redis;

public class RedisCacheSinkConnectorConfigTest extends BaseConnectorConfigTest<RedisSinkConnectorConfig> {
  @Override
  protected RedisSinkConnectorConfig newConnectorConfig() {
    return new RedisSinkConnectorConfig(this.settings);
  }
}
