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


import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.jcustenborder.kafka.connect.utils.data.SinkOffsetState;
import com.github.jcustenborder.kafka.connect.utils.jackson.ObjectMapperFactory;
import com.google.common.base.Charsets;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

public class RedisRecord {
  private static final Logger log = LoggerFactory.getLogger(RedisRecord.class);
  private final byte[] key;
  private final byte[] value;
  private final SinkOperation.Type type;

  RedisRecord(byte[] key, byte[] value, SinkOperation.Type type) {
    this.key = key;
    this.value = value;
    this.type = type;
  }

  public byte[] key() {
    return this.key;
  }

  public byte[] value() {
    return this.value;
  }

  public SinkOperation.Type type() {
    return this.type;
  }

  public static RedisRecord fromBatchableSinkRecord(SinkRecord record, RedisSinkConnectorConfig config) throws DataException {
    return new RedisRecord(
      keyFromRecord(record, config),
      toBytes("value", record.value(), config),
      record.value() == null ? SinkOperation.Type.DELETE : SinkOperation.Type.SET
    );
  }

  public static RedisRecord fromSinkRecord(SinkRecord record, RedisSinkConnectorConfig config) {
    return new RedisRecord(
      config.redisChannel.getBytes(StandardCharsets.UTF_8),
      toBytes("value", record.value(), config),
      SinkOperation.Type.PUBLISH
    );
  }

  public static RedisRecord fromSinkOffsetState(SinkOffsetState e) {
    final byte[] key = String.format("__kafka.offset.%s.%s", e.topic(), e.partition()).getBytes(Charsets.UTF_8);
    final byte[] value;
    try {
      value = ObjectMapperFactory.INSTANCE.writeValueAsBytes(e);
    } catch (JsonProcessingException e1) {
      throw new DataException(e1);
    }
    log.trace("RedisRecord::fromSinkOffsetState: Setting offset: {}", e);
    return new RedisRecord(key, value, SinkOperation.Type.SET);
  }

  private static byte[] keyFromRecord(SinkRecord record, RedisSinkConnectorConfig config) {
    log.trace("RedisRecord::keyFromRecord - Processing record " + formatLocation(record));
    if (record.key() == null) {
      throw new DataException(
        "The key for the record cannot be null. " + formatLocation(record)
      );
    }
    final byte[] key = toBytes("key", record.key(), config);
    if (null == key || key.length == 0) {
      throw new DataException(
        "The key cannot be an empty byte array. " + formatLocation(record)
      );
    }
    return key;
  }

  private static byte[] toBytes(String source, Object input, RedisSinkConnectorConfig config) {
    final byte[] result;
    if (input instanceof String) {
      String s = (String) input;
      result = s.getBytes(config.charset);
    } else if (input instanceof byte[]) {
      result = (byte[]) input;
    } else if (null == input) {
      result = null;
    } else {
      throw new DataException(
        String.format(
          "The %s for the record must be String or Bytes. Consider using the ByteArrayConverter " +
                  "or StringConverter if the data is stored in Kafka in the format needed in Redis. " +
                  "Another option is to use a single message transformation to transform the data before " +
                  "it is written to Redis.",
          source
        ));
    }
    return result;
  }

  static String formatLocation(SinkRecord record) {
    return String.format(
      "topic = %s partition = %s offset = %s",
      record.topic(),
      record.kafkaPartition(),
      record.kafkaOffset()
    );
  }
}
