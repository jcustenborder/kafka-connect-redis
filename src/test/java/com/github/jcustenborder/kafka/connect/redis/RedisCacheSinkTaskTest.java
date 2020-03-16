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

import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.github.jcustenborder.kafka.connect.utils.SinkRecordHelper.write;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public class RedisCacheSinkTaskTest {
  long offset = 1;
  RedisCacheSinkTask task;
  RedisClusterAsyncCommands<byte[], byte[]> asyncCommands;

  SinkRecord record(String k, String v) {
    final byte[] key = k.getBytes(Charsets.UTF_8);
    final Schema keySchema = Schema.BYTES_SCHEMA;
    final byte[] value;
    final Schema valueSchema;

    if (Strings.isNullOrEmpty(v)) {
      value = null;
      valueSchema = null;
    } else {
      value = v.getBytes(Charsets.UTF_8);
      valueSchema = Schema.BYTES_SCHEMA;
    }

    return new SinkRecord(
        "topic",
        1,
        keySchema,
        key,
        valueSchema,
        value,
        offset++
    );

  }

  @BeforeEach
  public void before() throws InterruptedException {
    this.task = new RedisCacheSinkTask();
    this.task.session = mock(RedisClusterSession.class);
    this.asyncCommands = mock(RedisAdvancedClusterAsyncCommands.class, withSettings().verboseLogging());
    when(task.session.asyncCommands()).thenReturn(asyncCommands);

    RedisFuture<String> setFuture = mock(RedisFuture.class);
    when(setFuture.await(anyLong(), any(TimeUnit.class))).thenReturn(true);
    RedisFuture<Long> deleteFuture = mock(RedisFuture.class);
    when(deleteFuture.await(anyLong(), any(TimeUnit.class))).thenReturn(true);
    when(asyncCommands.mset(anyMap())).thenReturn(setFuture);
    when(asyncCommands.del(any())).thenReturn(deleteFuture);
    task.config = new RedisCacheSinkConnectorConfig(
        ImmutableMap.of()
    );
  }


  @Test
  public void nonByteOrStringKey() {
    DataException exception = assertThrows(DataException.class, () -> {
      this.task.put(
          Arrays.asList(
              write("topic",
                  new SchemaAndValue(Schema.INT32_SCHEMA, 1),
                  new SchemaAndValue(Schema.INT32_SCHEMA, 1)
              )
          )
      );
    });
    assertEquals(
        "The key for the record must be String or Bytes. Consider using the ByteArrayConverter or StringConverter if the data is stored in Kafka in the format needed in Redis. Another option is to use a single message transformation to transform the data before it is written to Redis.",
        exception.getMessage());
  }

  @Test
  public void nonByteOrStringValue() {
    DataException exception = assertThrows(DataException.class, () -> {
      this.task.put(
          Arrays.asList(
              write("topic",
                  new SchemaAndValue(Schema.STRING_SCHEMA, "test"),
                  new SchemaAndValue(Schema.INT32_SCHEMA, 1)
              )
          )
      );
    });

    assertEquals(
        "The value for the record must be String or Bytes. Consider using the ByteArrayConverter or StringConverter if the data is stored in Kafka in the format needed in Redis. Another option is to use a single message transformation to transform the data before it is written to Redis.",
        exception.getMessage()
    );
  }

  @Test
  public void put() throws InterruptedException {
    List<SinkRecord> records = Arrays.asList(
        record("set1", "asdf"),
        record("set2", "asdf"),
        record("delete1", null),
        record("set3", "asdf"),
        record("set4", "asdf")
    );

    task.put(records);

    InOrder inOrder = Mockito.inOrder(asyncCommands);
    inOrder.verify(asyncCommands).mset(anyMap());
    inOrder.verify(asyncCommands).del(any(byte[].class));
    inOrder.verify(asyncCommands).mset(anyMap());
  }

}
