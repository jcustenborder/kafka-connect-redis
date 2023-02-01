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
import com.github.jcustenborder.kafka.connect.utils.config.Description;
import com.github.jcustenborder.kafka.connect.utils.config.recommenders.Recommenders;
import com.github.jcustenborder.kafka.connect.utils.config.validators.Validators;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

class RedisSinkConnectorConfig extends RedisConnectorConfig {

  public static final String OFFSET_RESET_BEHAVIOR_CONF = "redis.offset.behavior";
  public static final String OFFSET_RESET_BEHAVIOR_DOC = "This setting is used to control the behavior when the task(s) starts " +
      "and there are no offsets stored in Redis for the assigned partitions. " + ConfigUtils.enumDescription(OffsetReset.class);


  public final OffsetReset offsetReset;


  public RedisSinkConnectorConfig(Map<?, ?> originals) {
    super(config(), originals);
    this.offsetReset = ConfigUtils.getEnum(OffsetReset.class, this, OFFSET_RESET_BEHAVIOR_CONF);
  }

  public static ConfigDef config() {
    return RedisConnectorConfig.config()
        .define(
            ConfigKeyBuilder.of(OFFSET_RESET_BEHAVIOR_CONF, ConfigDef.Type.STRING)
                .documentation(OFFSET_RESET_BEHAVIOR_DOC)
                .recommender(Recommenders.enumValues(OffsetReset.class))
                .validator(Validators.validEnum(OffsetReset.class))
                .defaultValue(OffsetReset.Earliest.name())
                .importance(ConfigDef.Importance.LOW)
                .build()
        );
  }


  public enum OffsetReset {
    @Description("Reset the consumer to the earliest offset for the topic partition. Use this setting if you want to repopulate Redis when the offsets are not found. This typically happens when Redis is reset.")
    Earliest,
    @Description("Do not request an offset and use the default behavior of Kafka Connect.")
    None
  }
}
