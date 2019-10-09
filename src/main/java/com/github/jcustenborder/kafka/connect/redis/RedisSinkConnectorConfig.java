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

  public final static String INSERTION_TYPE_CONF = "redis.insertion.type";
  public final static String INSERTION_TYPE_DOC = "The type of insertion : SET, LPUSH, RPUSH";

  public final static String CONNECTION_ATTEMPTS_CONF = "redis.connection.attempts";
  public final static String CONNECTION_ATTEMPTS_DOC = "The number of attempt when connecting to redis";

  public final static String CONNECTION_RETRY_DELAY_MS_CONF = "redis.connection.retry.delay.ms";
  public final static String CONNECTION_RETRY_DELAY_MS_DOC = "The amount of milliseconds to wait between two redis connection attempt";

  public final long operationTimeoutMs;
  public final Charset charset;
  public final SinkOperation.Type type;
  public final int retryDelay;
  public final int maxAttempts;

  public RedisSinkConnectorConfig(Map<?, ?> originals) {
    super(config(), originals);
    this.operationTimeoutMs = getLong(OPERATION_TIMEOUT_MS_CONF);
    String charset = getString(CHARSET_CONF);
    this.charset = Charset.forName(charset);
    String type = getString(INSERTION_TYPE_CONF);
    this.type = SinkOperation.Type.valueOf(type);
    this.maxAttempts = getInt(CONNECTION_ATTEMPTS_CONF);
    this.retryDelay = getInt(CONNECTION_RETRY_DELAY_MS_CONF);
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
        ).define(
            ConfigKeyBuilder.of(INSERTION_TYPE_CONF, ConfigDef.Type.STRING)
                .documentation(INSERTION_TYPE_DOC)
                .defaultValue(SinkOperation.Type.SET.toString())
                .validator(Validators.validEnum(SinkOperation.Type.class))
                .recommender(Recommenders.enumValues(SinkOperation.Type.class))
                .importance(ConfigDef.Importance.MEDIUM)
                .build()
        ).define(
            ConfigKeyBuilder.of(CONNECTION_ATTEMPTS_CONF, ConfigDef.Type.INT)
                    .documentation(CONNECTION_ATTEMPTS_DOC)
                    .defaultValue(3)
                    .importance(ConfigDef.Importance.MEDIUM)
                    .build()
        ).define(
            ConfigKeyBuilder.of(CONNECTION_RETRY_DELAY_MS_CONF, ConfigDef.Type.INT)
                    .documentation(CONNECTION_RETRY_DELAY_MS_DOC)
                    .defaultValue(2000)
                    .validator(ConfigDef.Range.atLeast(100))
                    .importance(ConfigDef.Importance.MEDIUM)
                    .build()
        );
  }
}
