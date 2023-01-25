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

import java.util.Collections;
import java.util.Map;
import java.util.Set;

class RedisPubSubSourceConnectorConfig extends RedisSourceConnectorConfig {

  public static final String REDIS_CHANNELS_CONF = "redis.channels";
  static final String REDIS_CHANNELS_DOC = "redis.channels";

  public static final String REDIS_CHANNEL_PATTERNS_CONF = "redis.channel.patterns";
  static final String REDIS_CHANNEL_PATTERNS_DOC = "redis.channel.patterns";

  public static final String TOPIC_PREFIX_CONF = "redis.topic.prefix";
  static final String TOPIC_PREFIX_DOC = "Prefix that will be added the beginning of the channel name";
  public final Set<String> channels;
  public final Set<String> channelPatterns;
  public final String topicPrefix;

  public RedisPubSubSourceConnectorConfig(Map<?, ?> originals) {
    super(config(), originals);
    this.channels = ConfigUtils.getSet(this, REDIS_CHANNELS_CONF);
    this.channelPatterns = ConfigUtils.getSet(this, REDIS_CHANNEL_PATTERNS_CONF);
    this.topicPrefix = getString(TOPIC_PREFIX_CONF);
  }

  public static ConfigDef config() {
    return RedisConnectorConfig.config()
        .define(
            ConfigKeyBuilder.of(REDIS_CHANNELS_CONF, ConfigDef.Type.LIST)
                .documentation(REDIS_CHANNELS_DOC)
                .importance(ConfigDef.Importance.HIGH)
                .defaultValue(Collections.emptyList())
                .build()
        ).define(
            ConfigKeyBuilder.of(TOPIC_PREFIX_CONF, ConfigDef.Type.STRING)
                .documentation(TOPIC_PREFIX_DOC)
                .importance(ConfigDef.Importance.MEDIUM)
                .defaultValue("redis.")
                .build()
        ).define(
            ConfigKeyBuilder.of(REDIS_CHANNEL_PATTERNS_CONF, ConfigDef.Type.LIST)
                .documentation(REDIS_CHANNEL_PATTERNS_DOC)
                .importance(ConfigDef.Importance.HIGH)
                .defaultValue(Collections.emptyList())
                .build()
        );
  }
}
