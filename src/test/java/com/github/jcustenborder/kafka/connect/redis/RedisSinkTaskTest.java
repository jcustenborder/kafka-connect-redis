/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.redis;

import com.github.jcustenborder.kafka.connect.redis.RedisConnectorConfig.InsertMode;
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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.github.jcustenborder.kafka.connect.utils.SinkRecordHelper.write;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class RedisSinkTaskTest {
  private long offset = 1;

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

  private RedisSinkTask task;
  private RedisClusterAsyncCommands<byte[], byte[]> asyncCommands;

  @BeforeEach
  @SuppressWarnings("unchecked")
  public void before() throws InterruptedException {
    this.task = new RedisSinkTask();
    this.task.session = mock(RedisSession.class);
    this.asyncCommands = mock(RedisAdvancedClusterAsyncCommands.class, withSettings().verboseLogging());
    when(task.session.asyncCommands()).thenReturn(asyncCommands);

    RedisFuture<String> setFuture = mock(RedisFuture.class);
    when(setFuture.await(anyLong(), any(TimeUnit.class))).thenReturn(true);

    RedisFuture<Long> deleteFuture = mock(RedisFuture.class);
    when(deleteFuture.await(anyLong(), any(TimeUnit.class))).thenReturn(true);

    RedisFuture<Long> pushFuture = mock(RedisFuture.class);
    when(pushFuture.await(anyLong(), any(TimeUnit.class))).thenReturn(true);

    when(asyncCommands.mset(anyMap())).thenReturn(setFuture);
    when(asyncCommands.del(any())).thenReturn(deleteFuture);
    when(asyncCommands.lpush(any(byte[].class), any())).thenReturn(pushFuture);
    when(asyncCommands.rpush(any(byte[].class), any())).thenReturn(pushFuture);
    task.config = new RedisSinkConnectorConfig(
        ImmutableMap.of()
    );
  }


  @Test
  public void nonByteOrStringKey() {
    DataException exception = assertThrows(DataException.class, () -> {
      this.task.put(
          Collections.singletonList(
              write("topic",
                  new SchemaAndValue(Schema.INT32_SCHEMA, 1),
                  new SchemaAndValue(Schema.INT32_SCHEMA, 1)
              )
          )
      );
    });
    assertEquals(
        "The key for the record must be String, Bytes or schema-less Json. Consider using the ByteArrayConverter or StringConverter if the data is stored in Kafka in the format needed in Redis. Another option is to use a single message transformation to transform the data before it is written to Redis.",
        exception.getMessage());
  }

  @Test
  public void nonByteOrStringValue() {
    DataException exception = assertThrows(DataException.class, () -> {
      this.task.put(
          Collections.singletonList(
              write("topic",
                  new SchemaAndValue(Schema.STRING_SCHEMA, "test"),
                  new SchemaAndValue(Schema.INT32_SCHEMA, 1)
              )
          )
      );
    });

    assertEquals(
        "The value for the record must be String, Bytes or schema-less Json. Consider using the ByteArrayConverter or StringConverter if the data is stored in Kafka in the format needed in Redis. Another option is to use a single message transformation to transform the data before it is written to Redis.",
        exception.getMessage()
    );
  }

  @Test
  public void putWithSet() {
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
    inOrder.verify(asyncCommands, times(2)).mset(anyMap());
  }

  @Test
  public void putWithAppend() {
    List<SinkRecord> records = Arrays.asList(
        record("set1", "asdf"),
        record("set1", "wqer"),
        record("set2", "zxcv"),
        record("set2", "rtyj"),
        record("set2", "qwef"),
        record("set1", "bnms"),
        record("set1", "dfgh"),
        record("delete1", null),
        record("set2", "asdf"),
        record("set2", "qwer"),
        record("set1", "xzcv"),
        record("set1", "tyui"),
        record("set1", "ghjk"),
        record("set2", "asdf"),
        record("set2", "tyjd")
    );

    task.config = new RedisSinkConnectorConfig(
        ImmutableMap.of("redis.insert.mode", InsertMode.APPEND.toString())
    );
    task.put(records);

    InOrder inOrder = Mockito.inOrder(asyncCommands);
    inOrder.verify(asyncCommands, times(2)).lpush(any(byte[].class), any());
    inOrder.verify(asyncCommands).del(any(byte[].class));
    inOrder.verify(asyncCommands, times(2)).lpush(any(byte[].class), any());
    inOrder.verify(asyncCommands).mset(anyMap());
  }
}
