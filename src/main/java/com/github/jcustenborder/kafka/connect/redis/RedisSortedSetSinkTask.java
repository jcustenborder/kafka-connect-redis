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

import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

public class RedisSortedSetSinkTask extends BaseRedisSinkTask<RedisCacheSinkConnectorConfig> {
  private static final Logger log = LoggerFactory.getLogger(RedisSortedSetSinkTask.class);

  @Override
  protected RedisCacheSinkConnectorConfig config(Map<String, String> settings) {
    return new RedisCacheSinkConnectorConfig(settings);
  }

  @Override
  protected void operations(SinkOperations sinkOperations, Collection<SinkRecord> records) {
    for (SinkRecord record : records) {
      log.trace("put() - Processing record " + formatLocation(record));
      if (null == record.key()) {
        throw new DataException(
            "The key for the record cannot be null. " + formatLocation(record)
        );
      }
      final byte[] key = toBytes("key", record.key());
      if (null == key || key.length == 0) {
        throw new DataException(
            "The key cannot be an empty byte array. " + formatLocation(record)
        );
      }

      final byte[] value = toBytes("value", record.value());
      if (null == value) {
//        sinkOperations.srem(key);
      } else {
        sinkOperations.sadd(key, value);
      }
    }
  }
}
