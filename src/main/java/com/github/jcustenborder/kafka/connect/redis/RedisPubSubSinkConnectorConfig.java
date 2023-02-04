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

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class RedisPubSubSinkConnectorConfig extends RedisSinkConnectorConfig {

  //TODO correct this.
  public static final String TARGET_CONFIG = "redis.target";
  public static final String TARGET_DOC = "The location to write to.";

  public final Set<byte[]> target;

  public RedisPubSubSinkConnectorConfig(Map<?, ?> originals) {
    super(config(), originals);
    this.target = getList(TARGET_CONFIG).stream()
        .map(s -> s.getBytes(StandardCharsets.UTF_8))
        .collect(Collectors.toSet());
  }

  public static ConfigDef config() {
    return RedisSinkConnectorConfig.config()
        .define(
            ConfigKeyBuilder.of(TARGET_CONFIG, ConfigDef.Type.LIST)
                .importance(ConfigDef.Importance.HIGH)
                .documentation(TARGET_DOC)
                .build()
        );
  }
}
