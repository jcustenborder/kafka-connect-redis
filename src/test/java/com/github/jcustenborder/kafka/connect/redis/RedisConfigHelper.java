package com.github.jcustenborder.kafka.connect.redis;

import io.lettuce.core.RedisURI;

import java.util.Map;

public class RedisConfigHelper {
  public static RedisURI sentenielURI() {
    return RedisURI.Builder.sentinel("127.0.0.1", 51000)
        .withSentinel("127.0.0.1", 51001)
        .withSentinel("127.0.0.1", 51002)
        .withSentinelMasterId("mymaster")
        .build();
  }

  public static void sentenielSettings(Map<String, String> settings) {
    settings.put(RedisConnectorConfig.HOSTS_CONFIG, "127.0.0.1:51000,127.0.0.1:51001,127.0.0.1:51002");
    settings.put(RedisConnectorConfig.CLIENT_MODE_CONFIG, RedisConnectorConfig.ClientMode.Sentinel.name());
    settings.put(RedisPubSubSourceConnectorConfig.SENTINEL_MASTER_ID_CONFIG, "mymaster");
  }



}
