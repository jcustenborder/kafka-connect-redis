package com.github.jcustenborder.kafka.connect.redis.healthchecks;

import com.palantir.docker.compose.connection.waiting.ClusterHealthCheck;
import io.lettuce.core.api.sync.RedisStringCommands;

import java.util.Date;

public abstract class AbstractRedisHealthCheck implements ClusterHealthCheck {

  protected boolean testKeys(RedisStringCommands<String, String> commands) {
    String roundTrip = Long.toString(new Date().getTime());
    commands.set("healthcheck", roundTrip);
    String result = commands.get("healthcheck");
    return roundTrip.equals(result);
  }



}
