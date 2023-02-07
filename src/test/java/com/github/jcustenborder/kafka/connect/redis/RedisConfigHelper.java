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