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

import io.lettuce.core.GeoCoordinates;
import io.lettuce.core.GeoValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class RedisGeoSinkTask extends AbstractRedisCacheSinkTask<RedisSinkConnectorConfig> {
  private static final Logger log = LoggerFactory.getLogger(RedisGeoSinkTask.class);

  @Override
  protected RedisSinkConnectorConfig config(Map<String, String> settings) {
    return new RedisSinkConnectorConfig(settings);
  }

//  SinkOperation.GeoSetKey fromStructKey(Struct struct) {
//    byte[] key;
//    byte[] member;
//    Object fieldValue = struct.get("key");
//    key = toBytes("struct.key", fieldValue);
//    fieldValue = struct.get("member");
//    member = toBytes("struct.member", fieldValue);
//    return SinkOperation.GeoSetKey.of(key, member);
//  }
//
//  SinkOperation.Location fromStructValue(Struct struct, byte[] member) {
//    Number latitude = (Number) struct.get("latitude");
//    Number longitude = (Number) struct.get("longitude");
//    return SinkOperation.Location.of(longitude.doubleValue(), latitude.doubleValue(), member);
//  }

  Number number(Object value) {
    if (value instanceof Number) {
      return (Number) value;
    } else if (value instanceof String) {
      return Double.parseDouble(value.toString());
    } else if (null == value) {
      throw new DataException("value cannot be null");
    } else {
      throw new DataException(
          String.format("Could not convert '%s' to double", value.getClass().getName())
      );
    }
  }

  Number getNumber(String key, Map values) {
    Object value = values.get(key);
    try {
      return number(value);
    } catch (DataException ex) {
      throw new DataException(
          String.format("Could not convert '%s' to double", key),
          ex
      );
    }
  }

  Number getNumber(String key, Struct values) {
    Object value = values.get(key);
    try {
      return number(value);
    } catch (DataException ex) {
      throw new DataException(
          String.format("Could not convert '%s' to double", key),
          ex
      );
    }
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    Map<String, byte[]> topicToByte = new HashMap<>();

    for (SinkRecord record : records) {
      final byte[] topicKey = topicToByte.computeIfAbsent(record.topic(), t -> t.getBytes(StandardCharsets.UTF_8));
      log.trace("put() - Processing record " + Utils.formatLocation(record));

      if (null == record.key()) {
        throw new DataException(
            "The key for the record cannot be null. " + Utils.formatLocation(record)
        );
      }
      byte[] key = toBytes("key", record.key());

      CompletableFuture<?> future;

      if (null == record.value()) {
        future = this.session.asyncCommands().zrem(topicKey, key)
            .exceptionally(exceptionally(record))
            .toCompletableFuture();
      } else {
        Number latitude, longitude;

        if (record.value() instanceof Struct) {
          Struct struct = (Struct) record.value();
          latitude = getNumber("latitude", struct);
          longitude = getNumber("longitude", struct);
        } else if (record.value() instanceof Map) {
          Map map = (Map) record.value();
          latitude = getNumber("latitude", map);
          longitude = getNumber("longitude", map);
        } else {
          throw new UnsupportedOperationException();
        }

        GeoCoordinates coordinates = GeoCoordinates.create(latitude, longitude);
        GeoValue<byte[]> value = GeoValue.just(coordinates, key);

        future = this.session.asyncCommands().geoadd(topicKey, value)
            .exceptionally(exceptionally(record))
            .toCompletableFuture();
      }
      this.futures.add(future);
    }
  }
}
