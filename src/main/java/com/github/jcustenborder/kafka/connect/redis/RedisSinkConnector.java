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
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.List;
import java.util.Map;


@Description("Sink connector for writing data to Redis")
@DocumentationImportant("This connector expects to received data with a key of bytes and a values of " +
    "bytes. If your data is structured you need to use a Transformation to convert this data from " +
    "structured data like a Struct to an array of bytes for the key and value.")
@DocumentationNote("This connector supports deletes. It will issue a delete to the Redis cluster for " +
    "any key that does not have a corresponding value. In Kafka a record that contains a key and a " +
    "null value is considered a delete.")
public class RedisSinkConnector extends SinkConnector {
  @Override
  public String version() {
    return VersionUtil.version(this.getClass());
  }

  Map<String, String> settings;

  @Override
  public void start(Map<String, String> settings) {
    new RedisSinkConnectorConfig(settings);
    this.settings = settings;
  }

  @Override
  public Class<? extends Task> taskClass() {
    return RedisSinkTask.class;
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
    return RedisSinkConnectorConfig.config();
  }
}
