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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.github.jcustenborder.kafka.connect.redis.Utils.mapOf;

public class RedisCacheSinkConnectorConfig extends RedisSinkConnectorConfig {
  public static final String TTL_MS_CONFIG = "redis.ttl.milliseconds";
  public static final String TTL_SECONDS_CONFIG = "redis.ttl.seconds";
  public static final String TTL_MINUTES_CONFIG = "redis.ttl.minutes";
  public static final String TTL_HOURS_CONFIG = "redis.ttl.hours";
  public static final String TTL_DAYS_CONFIG = "redis.ttl.days";

  public static final String TTL_DOC = " The actual TTL value is a sum of `" + TTL_MS_CONFIG + "`, `" + TTL_SECONDS_CONFIG + "`, `" + TTL_MINUTES_CONFIG + "`, `" + TTL_HOURS_CONFIG + "`." + TTL_DAYS_CONFIG + "`.";

  public static final String TTL_MS_DOC = "Number of milliseconds to add to the TTL." + TTL_DOC;
  public static final String TTL_SECONDS_DOC = "Number of seconds to add to the TTL." + TTL_DOC;
  public static final String TTL_MINUTES_DOC = "Number of minutes to add to the TTL." + TTL_DOC;
  public static final String TTL_HOURS_DOC = "Number of hours to add to the TTL." + TTL_DOC;
  public static final String TTL_DAYS_DOC = "Number of days to add to the TTL." + TTL_DOC;

  static final Map<String, String> TTL_CONFIGS = mapOf(
      TTL_MS_CONFIG, TTL_MS_DOC,
      TTL_SECONDS_CONFIG, TTL_SECONDS_DOC,
      TTL_MINUTES_CONFIG, TTL_MINUTES_DOC,
      TTL_HOURS_CONFIG, TTL_HOURS_DOC,
      TTL_DAYS_CONFIG, TTL_DAYS_DOC
  );

  public final long ttl;

  public RedisCacheSinkConnectorConfig(Map<?, ?> originals) {
    super(config(), originals);
    Map<String, TimeUnit> timeUnits = new LinkedHashMap<>();
    timeUnits.put(TTL_MS_CONFIG, TimeUnit.MILLISECONDS);
    timeUnits.put(TTL_SECONDS_CONFIG, TimeUnit.SECONDS);
    timeUnits.put(TTL_MINUTES_CONFIG, TimeUnit.MINUTES);
    timeUnits.put(TTL_HOURS_CONFIG, TimeUnit.HOURS);
    timeUnits.put(TTL_DAYS_CONFIG, TimeUnit.DAYS);

    this.ttl = TTL_CONFIGS.keySet().stream()
        .map(s -> {
          long duration = getLong(s);
          TimeUnit timeUnit = timeUnits.get(s);
          return timeUnit.toMillis(duration);
        })
        .reduce(0L, Long::sum);
  }

  public static ConfigDef config() {
    ConfigDef result = RedisSinkConnectorConfig.config();

    for (Map.Entry<String, String> kvp : TTL_CONFIGS.entrySet()) {
      result = result.define(
          ConfigKeyBuilder.of(kvp.getKey(), ConfigDef.Type.LONG)
              .importance(ConfigDef.Importance.LOW)
              .documentation(kvp.getValue())
              .defaultValue(0)
              .validator(ConfigDef.Range.atLeast(0))
              .build()
      );
    }

    return result;
  }
}
