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

public class RedisGeoSinkTask extends AbstractRedisSinkTask<RedisSinkConnectorConfig> {
  private static final Logger log = LoggerFactory.getLogger(RedisGeoSinkTask.class);

  @Override
  protected RedisSinkConnectorConfig config(Map<String, String> settings) {
    return new RedisSinkConnectorConfig(settings);
  }

  SinkOperation.GeoSetKey fromStructKey(Struct struct) {
    byte[] key;
    byte[] member;
    Object fieldValue = struct.get("key");
    key = toBytes("struct.key", fieldValue);
    fieldValue = struct.get("member");
    member = toBytes("struct.member", fieldValue);
    return SinkOperation.GeoSetKey.of(key, member);
  }

  SinkOperation.Location fromStructValue(Struct struct, byte[] member) {
    Number latitude = (Number) struct.get("latitude");
    Number longitude = (Number) struct.get("longitude");
    return SinkOperation.Location.of(longitude.doubleValue(), latitude.doubleValue(), member);
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
      SinkOperation.GeoSetKey key;

      if (record.key() instanceof Struct) {
        key = fromStructKey((Struct) record.key());
      } else if (record.key() instanceof Map) {
        throw new UnsupportedOperationException();
      } else {
        throw new UnsupportedOperationException();
      }


      if (null == record.value()) {
        sinkOperations.zrem(key);
      } else if (record.value() instanceof Struct) {
        Struct struct = (Struct) record.value();
        SinkOperation.Location location = fromStructValue(struct, key.member);
        sinkOperations.geoadd(key, location);
      } else {
        throw new DataException(
            "The value for the record must be a Struct or Map." + formatLocation(record)
        );
      }
    }
  }
}
