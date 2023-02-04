/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.redis;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.cluster.ClusterClientOptions;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.github.jcustenborder.kafka.connect.redis.TestUtils.mapOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class RedisSessionFactoryImplTest {


  RedisSinkConnectorConfig config(String... args) {
    Map<String, String> settings = mapOf(args);
    settings.put(RedisConnectorConfig.HOSTS_CONFIG, "127.0.0.1:12354");
    return new RedisCacheSinkConnectorConfig(settings);
  }

  <T extends ClientOptions> T clientOptions(Class<T> cls, String... args) {
    RedisSinkConnectorConfig config = config(args);
    T clientOptions = RedisSessionFactoryImpl.createClientOptions(config, cls);
    assertNotNull(clientOptions, "clientOptions should not be null.");
    assertEquals(cls, clientOptions.getClass(), "class does not match expected.");
    return clientOptions;
  }

  @Test
  public void clusterSSLEnabled() {
    ClusterClientOptions clientOptions = clientOptions(ClusterClientOptions.class,
        RedisSinkConnectorConfig.CLIENT_MODE_CONFIG, RedisConnectorConfig.ClientMode.Cluster.name(),
        RedisSinkConnectorConfig.SSL_CONFIG, Boolean.toString(true)
    );
    assertNotNull(clientOptions.getSslOptions());
  }

  @Test
  public void standardSSLEnabled() {
    ClientOptions clientOptions = clientOptions(ClientOptions.class,
        RedisSinkConnectorConfig.SSL_CONFIG, Boolean.toString(true)
    );
    assertNotNull(clientOptions.getSslOptions());
  }

  @Test
  public void foo() {


  }

}
