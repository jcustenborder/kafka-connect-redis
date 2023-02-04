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
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class RedisCacheSinkConnectorConfig extends RedisSinkConnectorConfig {
  public static final String TTL_CONFIG = "redis.ttl.milliseconds";
  public static final String TTL_DOC = "Set the key to timeout after the configured number of MS";

  public final long ttl;

  public RedisCacheSinkConnectorConfig(Map<?, ?> originals) {
    super(config(), originals);
    this.ttl = getLong(TTL_CONFIG);
  }

  public static ConfigDef config() {
    return RedisSinkConnectorConfig.config()
        .define(
            ConfigKeyBuilder.of(TTL_CONFIG, ConfigDef.Type.LONG)
                .importance(ConfigDef.Importance.LOW)
                .documentation(TTL_DOC)
                .defaultValue(0)
                .validator(ConfigDef.Range.atLeast(0))
                .build()
        );
  }
}
