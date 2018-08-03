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
import com.github.jcustenborder.kafka.connect.utils.config.recommenders.Recommenders;
import com.github.jcustenborder.kafka.connect.utils.config.validators.Validators;
import org.apache.kafka.common.config.ConfigDef;

import java.nio.charset.Charset;
import java.util.Map;

class RedisSinkConnectorConfig extends RedisConnectorConfig {

  public final static String OPERATION_TIMEOUT_MS_CONF = "redis.operation.timeout.ms";
  final static String OPERATION_TIMEOUT_MS_DOC = "The amount of time in milliseconds before an" +
      " operation is marked as timed out.";

  public final static String CHARSET_CONF = "redis.charset";
  public final static String CHARSET_DOC = "The character set to use for String key and values.";

  public final long operationTimeoutMs;
  public final Charset charset;

  public RedisSinkConnectorConfig(Map<?, ?> originals) {
    super(config(), originals);
    this.operationTimeoutMs = getLong(OPERATION_TIMEOUT_MS_CONF);
    String charset = getString(CHARSET_CONF);
    this.charset = Charset.forName(charset);
  }

  public static ConfigDef config() {
    return RedisConnectorConfig.config()
        .define(
            ConfigKeyBuilder.of(OPERATION_TIMEOUT_MS_CONF, ConfigDef.Type.LONG)
                .documentation(OPERATION_TIMEOUT_MS_DOC)
                .defaultValue(10000L)
                .validator(ConfigDef.Range.atLeast(100L))
                .importance(ConfigDef.Importance.MEDIUM)
                .build()
        ).define(
            ConfigKeyBuilder.of(CHARSET_CONF, ConfigDef.Type.STRING)
                .documentation(CHARSET_DOC)
                .defaultValue("UTF-8")
                .validator(Validators.validCharset())
                .recommender(Recommenders.charset())
                .importance(ConfigDef.Importance.LOW)
                .build()
        );
  }
}
