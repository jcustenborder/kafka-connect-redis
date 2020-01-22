package com.github.jcustenborder.kafka.connect.redis;

interface RedisSessionFactory {
  RedisSession create(RedisConnectorConfig config);
}
