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
import com.google.common.base.Strings;
import com.google.common.net.HostAndPort;
import io.lettuce.core.RedisURI;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

class RedisConnectorConfig extends AbstractConfig {
  public static final String HOSTS_CONFIG = "redis.hosts";
  static final String HOSTS_DOC = "";
  public static final String SSL_CONFIG = "redis.ssl.enabled";
  static final String SSL_DOC = "";
  public static final String PASSWORD_CONFIG = "redis.password";
  static final String PASSWORD_DOC = "";
  public static final String DATABASE_CONFIG = "redis.database";
  static final String DATABASE_DOC = "";

  public final List<HostAndPort> hosts;
  public final boolean ssl;
  public final String password;
  public final int database;

  public RedisConnectorConfig(ConfigDef config, Map<?, ?> originals) {
    super(config, originals);
    this.hosts = ConfigUtils.hostAndPorts(this, HOSTS_CONFIG, 6379);
    this.ssl = getBoolean(SSL_CONFIG);
    this.password = getPassword(PASSWORD_CONFIG).value();
    this.database = getInt(DATABASE_CONFIG);
  }

  public static ConfigDef config() {
    return new ConfigDef()
        .define(
            ConfigKeyBuilder.of(HOSTS_CONFIG, ConfigDef.Type.LIST)
                .documentation(HOSTS_DOC)
                .defaultValue(Arrays.asList("localhost:6379"))
                .importance(ConfigDef.Importance.HIGH)
                .build()
        ).define(
            ConfigKeyBuilder.of(SSL_CONFIG, ConfigDef.Type.BOOLEAN)
                .documentation(SSL_DOC)
                .defaultValue(false)
                .importance(ConfigDef.Importance.MEDIUM)
                .build()
        ).define(
            ConfigKeyBuilder.of(PASSWORD_CONFIG, ConfigDef.Type.PASSWORD)
                .documentation(PASSWORD_DOC)
                .defaultValue("")
                .importance(ConfigDef.Importance.MEDIUM)
                .build()
        ).define(
            ConfigKeyBuilder.of(DATABASE_CONFIG, ConfigDef.Type.INT)
                .documentation(DATABASE_DOC)
                .defaultValue(1)
                .importance(ConfigDef.Importance.MEDIUM)
                .build()
        );
  }

  public List<RedisURI> redisURIs() {
    List<RedisURI> result = new ArrayList<>();

    for (HostAndPort host : this.hosts) {
      RedisURI.Builder builder = RedisURI.builder();
      builder.withHost(host.getHostText());
      builder.withPort(host.getPort());
      if (!Strings.isNullOrEmpty(this.password)) {
        builder.withPassword(this.password);
      }
      builder.withSsl(this.ssl);
    }

    return result;
  }
}
