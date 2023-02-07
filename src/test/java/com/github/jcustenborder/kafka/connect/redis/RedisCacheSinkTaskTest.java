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

import io.lettuce.core.RedisFuture;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static com.github.jcustenborder.kafka.connect.redis.TestUtils.offsets;
import static com.github.jcustenborder.kafka.connect.utils.SinkRecordHelper.write;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public class RedisCacheSinkTaskTest extends AbstractSinkTaskTest<RedisCacheSinkTask> {
  @Override
  protected RedisCacheSinkTask createTask() {
    return new RedisCacheSinkTask();
  }

  @Test
  public void nonByteOrStringKey() {
    this.task.start(this.settings);
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
        "The key for the record must be String or Bytes. Consider using the ByteArrayConverter or StringConverter if the data is stored in Kafka in the format needed in Redis. Another option is to use a single message transformation to transform the data before it is written to Redis.",
        exception.getMessage());
  }

  @Test
  public void nonByteOrStringValue() {
    this.task.start(this.settings);
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
        "The value for the record must be String or Bytes. Consider using the ByteArrayConverter or StringConverter if the data is stored in Kafka in the format needed in Redis. Another option is to use a single message transformation to transform the data before it is written to Redis.",
        exception.getMessage()
    );
  }


  @BeforeEach
  public void setupMocks() {
    when(this.string.set(any(byte[].class), any(byte[].class))).thenAnswer(invocationOnMock -> completedFuture(null));
    when(this.string.psetex(any(byte[].class), anyLong(), any(byte[].class))).thenAnswer(invocationOnMock -> completedFuture(null));
    when(this.session.key().del(any(byte[].class))).thenAnswer(invocationOnMock -> completedFuture(null));
    when(this.session.hash().hset(any(byte[].class), anyMap())).thenAnswer(invocationOnMock -> completedFuture(null));
  }


  @Test
  public void put() throws InterruptedException {
    this.task.start(this.settings);
    List<SinkRecord> records = Arrays.asList(
        record("set1", "asdf"),
        record("set2", "asdf"),
        record("delete1", null),
        record("set3", "asdf"),
        record("set4", "asdf")
    );

    this.task.put(records);
    Map<TopicPartition, OffsetAndMetadata> offsets = offsets(records);
    this.task.flush(offsets);

    InOrder inOrder = Mockito.inOrder(this.string, this.key, this.hash);
    inOrder.verify(this.string, times(2)).set(any(), any());
    inOrder.verify(this.key).del(any(byte[].class));
    inOrder.verify(this.string, times(2)).set(any(), any());
    inOrder.verify(this.hash).hset(any(), anyMap());
  }

  @Test
  public void putTTL() throws InterruptedException {
    this.settings.put(RedisCacheSinkConnectorConfig.TTL_SECONDS_CONFIG, "30");
    this.task.start(this.settings);
    List<SinkRecord> records = Arrays.asList(
        record("set1", "asdf"),
        record("set2", "asdf"),
        record("delete1", null),
        record("set3", "asdf"),
        record("set4", "asdf")
    );

    this.task.put(records);

    Map<TopicPartition, OffsetAndMetadata> offsets = offsets(records);
    this.task.flush(offsets);

    InOrder inOrder = Mockito.inOrder(this.string, this.key, this.hash);
    inOrder.verify(this.string, times(2)).psetex(any(), eq(this.task.config.ttl), any());
    inOrder.verify(this.key).del(any(byte[].class));
    inOrder.verify(this.string, times(2)).psetex(any(), eq(this.task.config.ttl), any());
    inOrder.verify(this.hash).hset(any(), anyMap());
  }


  static <T> RedisFuture<T> completedFuture(T value) throws ExecutionException, InterruptedException, TimeoutException {
    RedisFuture<T> result = mock(RedisFuture.class, withSettings().verboseLogging());
    when(result.get()).thenReturn(value);
    when(result.get(anyLong(), any())).thenReturn(value);
    when(result.exceptionally(any())).thenReturn(result);
    when(result.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture(value));

    return result;
  }

}
