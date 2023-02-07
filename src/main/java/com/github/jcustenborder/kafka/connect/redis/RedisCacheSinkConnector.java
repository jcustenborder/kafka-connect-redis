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
import com.github.jcustenborder.kafka.connect.utils.config.DocumentationImportant;
import com.github.jcustenborder.kafka.connect.utils.config.DocumentationNote;
import com.github.jcustenborder.kafka.connect.utils.config.DocumentationSection;
import com.github.jcustenborder.kafka.connect.utils.config.DocumentationSections;
import com.github.jcustenborder.kafka.connect.utils.config.Title;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;

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
@DocumentationSections(
    sections = {
        @DocumentationSection(title = "Additional Data Formats", text = "You might be asking, why does this connector only support the " +
            "StringConverter and the ByteArrayConverter. I really need the data to be JSON or <insert format name here>. " +
            "The keys for Redis need to be correct at the byte level. The safest way to accomplish this is utilize a Single Message Transformation (SMT)" +
            "that is controlled by the user. This allows the user to make a decision of how the data is stored in Redis using configuration of the connector. " +
            "For example data could be stored in Kafka as AVRO and you could use a SMT to convert this data to JSON as it's written to Redis.")
    }
)
public class RedisCacheSinkConnector extends AbstractRedisSinkConnector<RedisSinkConnectorConfig> {
  @Override
  protected RedisSinkConnectorConfig config(Map<String, String> settings) {
    return new RedisCacheSinkConnectorConfig(settings);
  }

  @Override
  public Class<? extends Task> taskClass() {
    return RedisCacheSinkTask.class;
  }

  @Override
  public ConfigDef config() {
    return RedisCacheSinkConnectorConfig.config();
  }
}
