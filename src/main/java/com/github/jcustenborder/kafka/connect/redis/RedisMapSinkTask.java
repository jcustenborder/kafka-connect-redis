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
import java.util.Map;

import static com.github.jcustenborder.kafka.connect.redis.Utils.formatLocation;
import static com.github.jcustenborder.kafka.connect.redis.Utils.logLocation;

public class RedisMapSinkTask extends AbstractRedisCacheSinkTask<RedisCacheSinkConnectorConfig> {
  private static final Logger log = LoggerFactory.getLogger(RedisMapSinkTask.class);

  @Override
  protected RedisCacheSinkConnectorConfig config(Map<String, String> settings) {
    return new RedisCacheSinkConnectorConfig(settings);
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    for (SinkRecord record : records) {
      logLocation(log, record);

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


      if (null == record.value()) {
        futures.add(
            this.session.asyncCommands().del(key)
                .exceptionally(exceptionally(record))
                .toCompletableFuture()
        );
      } else if (record.value() instanceof Struct) {
        Struct struct = (Struct) record.value();
        struct.schema().fields().forEach(field -> {
          Object v = struct.get(field);
          byte[] fieldKey = toBytes("field.key", field.name());
          if (null != v) {
            futures.add(
                this.session.asyncCommands().hdel(key, fieldKey)
                    .exceptionally(exceptionally(record))
                    .toCompletableFuture()
            );
          } else {
            byte[] fieldValue = toBytes("field.value", v.toString());
            futures.add(
                this.session.asyncCommands().hset(key, fieldKey, fieldValue)
                    .exceptionally(exceptionally(record))
                    .toCompletableFuture()
            );
          }
        });

      } else if (record.value() instanceof Map) {
        Map<String, Object> map = (Map) record.value();
        map.forEach((k, v) -> {
          byte[] fieldKey = toBytes("field.key", k);
          if (null != v) {
            futures.add(
                this.session.asyncCommands().hdel(key, fieldKey)
                    .exceptionally(exceptionally(record))
                    .toCompletableFuture()
            );
          } else {
            byte[] fieldValue = toBytes("field.value", v.toString());
            futures.add(
                this.session.asyncCommands().hset(key, fieldKey, fieldValue)
                    .exceptionally(exceptionally(record))
                    .toCompletableFuture()
            );
          }
        });
      } else {
        throw new DataException(
            "The value for the record must be a Struct or Map." + formatLocation(record)
        );
      }
    }
    log.debug("put() - Added {} future(s).", this.futures.size());
  }
}
