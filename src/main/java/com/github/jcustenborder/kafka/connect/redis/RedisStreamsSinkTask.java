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

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

public class RedisStreamsSinkTask extends AbstractRedisCacheSinkTask<RedisSinkConnectorConfig> {
  private static final Logger log = LoggerFactory.getLogger(RedisStreamsSinkTask.class);

  @Override
  protected RedisSinkConnectorConfig config(Map<String, String> settings) {
    return new RedisSinkConnectorConfig(settings);
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    for (SinkRecord record : records) {
      log.trace("put() - Processing record " + Utils.formatLocation(record));
      if (null == record.value()) {

      } else if (record.value() instanceof Struct) {
        Struct struct = (Struct) record.value();
        Map<byte[], byte[]> mapValues = new LinkedHashMap<>();
        struct.schema().fields().forEach(field -> {
          Object v = struct.get(field);
          byte[] fieldKey = toBytes("field.key", field.name());
          byte[] fieldValue = toBytes("field.value", v);
          mapValues.put(fieldKey, fieldValue);
        });
//        sinkOperations.xadd(record.topic(), mapValues);
      } else if (record.value() instanceof Map) {
        Map map = (Map) record.value();
        Map<byte[], byte[]> mapValues = new LinkedHashMap<>();
        map.forEach((k, v) -> {
          byte[] fieldKey = toBytes("field.key", k);
          byte[] fieldValue = toBytes("field.value", v);
          mapValues.put(fieldKey, fieldValue);
        });
//        sinkOperations.xadd(record.topic(), mapValues);
      } else {
        throw new DataException(
            "The value for the record must be a Struct or Map." + Utils.formatLocation(record)
        );
      }
    }
  }
}
