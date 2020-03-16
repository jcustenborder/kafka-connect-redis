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

@Title("Redis Cache Sink Connector")
@Description("The Redis Cache Sink Connector is used to write data from Kafka to a Redis cache.")
@DocumentationImportant("This connector expects records from Kafka to have a key and value that are " +
    "stored as bytes or a string. If your data is already in Kafka in the format that you want in " +
    "Redis consider using the ByteArrayConverter or the StringConverter for this connector. Keep in " +
    "this does not need to be configured in the worker properties and can be configured at the " +
    "connector level. If your data is not sitting in Kafka in the format you wish to persist in Redis " +
    "consider using a Single Message Transformation to convert the data to a byte or string representation " +
    "before it is written to Redis.")
@DocumentationNote("This connector supports deletes. If the record stored in Kafka has a null value, " +
    "this connector will send a delete with the corresponding key to Redis.")
public class RedisCacheSinkConnector extends SinkConnector {
  private static final Logger log = LoggerFactory.getLogger(RedisCacheSinkConnector.class);
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
    return RedisCacheSinkTask.class;
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
