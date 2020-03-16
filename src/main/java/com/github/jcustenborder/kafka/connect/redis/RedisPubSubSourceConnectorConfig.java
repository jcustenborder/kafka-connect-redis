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

import com.github.jcustenborder.kafka.connect.utils.config.ConfigKeyBuilder;
import com.github.jcustenborder.kafka.connect.utils.config.ConfigUtils;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;
import java.util.Set;

class RedisPubSubSourceConnectorConfig extends RedisConnectorConfig {

  public static final String REDIS_CHANNEL_CONF = "redis.channel";
  static final String REDIS_CHANNEL_DOC = "redis.channel";
  final Set<String> channels;

  public RedisPubSubSourceConnectorConfig(Map<?, ?> originals) {
    super(config(), originals);
    this.channels = ConfigUtils.getSet(this, REDIS_CHANNEL_CONF);
  }

  public static ConfigDef config() {
    return RedisConnectorConfig.config()
        .define(
            ConfigKeyBuilder.of(REDIS_CHANNEL_CONF, ConfigDef.Type.LIST)
                .documentation(REDIS_CHANNEL_DOC)
                .importance(ConfigDef.Importance.HIGH)
                .build()
        );
  }
}
