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

import com.github.jcustenborder.kafka.connect.utils.config.Description;
import com.github.jcustenborder.kafka.connect.utils.config.Title;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;

import java.util.Map;

@Title("Redis Pub/Sub Source Connector")
@Description("The Redis Pub/Sub Source Connector is used to retrieve data using Redis Pub/Sub subscriptions.")
public class RedisPubSubSourceConnector extends AbstractRedisSourceConnector<RedisPubSubSourceConnectorConfig> {
  @Override
  public Class<? extends Task> taskClass() {
    return RedisPubSubSourceTask.class;
  }

  @Override
  public ConfigDef config() {
    return RedisSinkConnectorConfig.config();
  }

  @Override
  protected RedisPubSubSourceConnectorConfig config(Map<String, String> settings) {
    return new RedisPubSubSourceConnectorConfig(settings);
  }
}
