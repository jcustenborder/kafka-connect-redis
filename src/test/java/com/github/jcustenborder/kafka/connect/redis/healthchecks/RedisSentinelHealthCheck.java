package com.github.jcustenborder.kafka.connect.redis.healthchecks;

import com.github.jcustenborder.kafka.connect.redis.RedisConfigHelper;
import com.palantir.docker.compose.connection.Cluster;
import com.palantir.docker.compose.connection.waiting.ClusterHealthCheck;
import com.palantir.docker.compose.connection.waiting.SuccessOrFailure;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.sentinel.api.StatefulRedisSentinelConnection;
import io.lettuce.core.sentinel.api.sync.RedisSentinelCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.StringReader;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RedisSentinelHealthCheck implements ClusterHealthCheck {
  private static final Logger log = LoggerFactory.getLogger(RedisSentinelHealthCheck.class);

  @Override
  public SuccessOrFailure isClusterHealthy(Cluster cluster) throws InterruptedException {
    return SuccessOrFailure.onResultOf(() -> {

      SuccessOrFailure allPortsOpen = cluster.container("redis").areAllPortsOpen();
      if (allPortsOpen.failed()) {
        return allPortsOpen.failed();
      }

      RedisURI redisURI = RedisConfigHelper.sentenielURI();
      log.info("Connecting to {}", redisURI);

//      RedisURI redisUri = RedisURI.Builder.sentinel("127.0.0.1", 51000)
//          .withSentinel("127.0.0.1", 51001)
//          .withSentinel("127.0.0.1", 51002)
//          .withSentinelMasterId("mymaster")
//          .build();

//      RedisClient client = RedisClient.create(redisUri);
//      StatefulRedisConnection<String, String> connection = client.connect();
//      RedisAsyncCommands<String, String> commands = connection.async();
//      RedisFuture<String> future = commands.set("test", "value");
//      future.get();
//

      try (RedisClient client = RedisClient.create(redisURI)) {

        try (StatefulRedisSentinelConnection<String, String> connection = client.connectSentinel()) {
          RedisSentinelCommands<String, String> syncCommands = connection.sync();

          String info = syncCommands.info();
          log.info(info);
          List<Map<String, String>> masters = syncCommands.masters();
          log.info("masters = {}", masters);


          return true;


//          Pattern pattern = Pattern.compile("(.+):(.+)");
//          String clusterInfo = syncCommands.clusterInfo();
//          log.trace("Cluster info:\n{}", clusterInfo);
//          try (StringReader reader = new StringReader(clusterInfo)) {
//            try (BufferedReader bufferedReader = new BufferedReader(reader)) {
//              String line;
//              while ((line = bufferedReader.readLine()) != null) {
//                Matcher matcher = pattern.matcher(line);
//                if (matcher.find()) {
//                  String key = matcher.group(1);
//                  String value = matcher.group(2);
//
//                  if ("cluster_state".equals(key) && "ok".equals(value)) {
//                    return true;
//                  }
//                }
//              }
//            }
//          }
//          return false;
        } catch (Exception ex) {
          log.debug("Exception thrown creating connection", ex);
          throw ex;
        }
      } catch (Exception ex) {
        log.debug("Exception thrown creating client", ex);
        throw ex;
      }
    });
  }
}
