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

import com.github.jcustenborder.kafka.connect.utils.VersionUtil;
import com.github.jcustenborder.kafka.connect.utils.config.Description;
import com.github.jcustenborder.kafka.connect.utils.config.DocumentationImportant;
import com.github.jcustenborder.kafka.connect.utils.config.DocumentationNote;
import com.github.jcustenborder.kafka.connect.utils.config.TaskConfigs;
import com.github.jcustenborder.kafka.connect.utils.config.Title;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

@Title("Redis Geo Sink Connector")
@Description("The Redis Geo Sink Connector is used to write data from Kafka to a Redis cache.")
@DocumentationImportant("This connector expects data to be written in the following format. " +
    "Key: `{\"key\":\"cities\", \"member\":\"Austin\"}`, " +
    "Value: `{\"latitude\":30.2672, \"longitude\":97.7431}` ")
@DocumentationNote("This connector supports deletes. If the record stored in Kafka has a null value, " +
    "this connector will send a delete with the corresponding key to Redis.")
public class RedisGeoSinkConnector extends SinkConnector {
  private static final Logger log = LoggerFactory.getLogger(RedisGeoSinkConnector.class);
  Map<String, String> settings;

  @Override
  public String version() {
    return VersionUtil.version(this.getClass());
  }

  @Override
  public void start(Map<String, String> settings) {
    new RedisCacheSinkConnectorConfig(settings);
    this.settings = settings;
  }

  @Override
  public Class<? extends Task> taskClass() {
    return RedisGeoSinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int count) {
    return TaskConfigs.multiple(this.settings, count);
  }

  @Override
  public void stop() {

  }

  @Override
  public ConfigDef config() {
    return RedisCacheSinkConnectorConfig.config();
  }
}
