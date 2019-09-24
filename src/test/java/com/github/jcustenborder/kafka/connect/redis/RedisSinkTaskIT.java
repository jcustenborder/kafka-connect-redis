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

import com.github.jcustenborder.docker.junit5.Compose;
import com.github.jcustenborder.docker.junit5.Port;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.github.jcustenborder.kafka.connect.utils.SinkRecordHelper.delete;
import static com.github.jcustenborder.kafka.connect.utils.SinkRecordHelper.write;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Compose(
    dockerComposePath = "src/test/resources/docker-compose.yml"
)
public class RedisSinkTaskIT {
  private static final Logger log = LoggerFactory.getLogger(RedisSinkTaskIT.class);


  RedisSinkTask task;

  @BeforeEach
  public void before() {
    this.task = new RedisSinkTask();
  }

  @Test
  public void emptyAssignment(@Port(container = "redis", internalPort = 6379) InetSocketAddress address) throws ExecutionException, InterruptedException {
    log.info("address = {}", address);
    final String topic = "putWrite";
    SinkTaskContext context = mock(SinkTaskContext.class);
    when(context.assignment()).thenReturn(ImmutableSet.of());
    this.task.initialize(context);
    this.task.start(
        ImmutableMap.of(RedisSinkConnectorConfig.HOSTS_CONFIG, String.format("%s:%s", address.getHostString(), address.getPort()))
    );
  }

  @Test
  public void putEmpty(@Port(container = "redis", internalPort = 6379) InetSocketAddress address) throws ExecutionException, InterruptedException {
    log.info("address = {}", address);
    final String topic = "putWrite";
    SinkTaskContext context = mock(SinkTaskContext.class);
    when(context.assignment()).thenReturn(ImmutableSet.of(new TopicPartition(topic, 1)));
    this.task.initialize(context);
    this.task.start(
        ImmutableMap.of(RedisSinkConnectorConfig.HOSTS_CONFIG, String.format("%s:%s", address.getHostString(), address.getPort()))
    );

    this.task.put(ImmutableList.of());
  }

  @Test
  public void putWrite(@Port(container = "redis", internalPort = 6379) InetSocketAddress address) throws ExecutionException, InterruptedException {
    log.info("address = {}", address);
    final String topic = "putWrite";
    SinkTaskContext context = mock(SinkTaskContext.class);
    when(context.assignment()).thenReturn(ImmutableSet.of(new TopicPartition(topic, 1)));
    this.task.initialize(context);
    this.task.start(
        ImmutableMap.of(RedisSinkConnectorConfig.HOSTS_CONFIG, String.format("%s:%s", address.getHostString(), address.getPort()))
    );

    final int count = 50;
    final Map<String, String> expected = new LinkedHashMap<>(count);
    final List<SinkRecord> records = new ArrayList<>(count);

    for (int i = 0; i < count; i++) {
      final String key = String.format("putWrite%s", i);
      final String value = String.format("This is value %s", i);
      records.add(
          write(topic,
              new SchemaAndValue(Schema.STRING_SCHEMA, key),
              new SchemaAndValue(Schema.STRING_SCHEMA, value)
          )
      );

      expected.put(key, value);
    }
    this.task.put(records);

    final byte[][] keys = expected.keySet().stream()
        .map(s -> s.getBytes(Charsets.UTF_8))
        .toArray(byte[][]::new);
    RedisFuture<List<KeyValue<byte[], byte[]>>> result = this.task.session.asyncCommands().mget(keys);
    List<KeyValue<byte[], byte[]>> actual = result.get();
    assertEquals(count, actual.size());
    for (KeyValue<byte[], byte[]> kv : actual) {
      final String key = new String(kv.getKey(), Charsets.UTF_8);
      final String value = new String(kv.getValue(), Charsets.UTF_8);
      assertEquals(value, expected.get(key), String.format("Value for key(%s) does not match.", key));
    }
  }

  @Test
  public void putDelete(@Port(container = "redis", internalPort = 6379) InetSocketAddress address) throws ExecutionException, InterruptedException {
    log.info("address = {}", address);
    final String topic = "putDelete";
    SinkTaskContext context = mock(SinkTaskContext.class);
    when(context.assignment()).thenReturn(ImmutableSet.of(new TopicPartition(topic, 1)));
    this.task.initialize(context);
    this.task.start(
        ImmutableMap.of(RedisSinkConnectorConfig.HOSTS_CONFIG, String.format("%s:%s", address.getHostString(), address.getPort()))
    );

    final int count = 50;
    final Map<String, String> expected = new LinkedHashMap<>(count);
    final List<SinkRecord> records = new ArrayList<>(count);

    for (int i = 0; i < count; i++) {
      final String key = String.format("putDelete%s", i);
      final String value = String.format("This is value %s", i);

      records.add(
          delete(topic,
              new SchemaAndValue(Schema.STRING_SCHEMA, key)
          )
      );

      expected.put(key, value);
    }
    final Map<byte[], byte[]> values = expected.entrySet().stream()
        .collect(Collectors.toMap(
            kv -> kv.getKey().getBytes(Charsets.UTF_8),
            kv -> kv.getValue().getBytes(Charsets.UTF_8)
        ));

    this.task.session.asyncCommands().mset(values).get();
    this.task.put(records);
    final byte[][] keys = expected.keySet().stream()
        .map(s -> s.getBytes(Charsets.UTF_8))
        .toArray(byte[][]::new);
    final long actual = this.task.session.asyncCommands().exists(keys).get();
    assertEquals(0L, actual, "All of the keys should be removed from Redis.");
  }

  @Test
  public void lpushWrite(@Port(container = "redis", internalPort = 6379) InetSocketAddress address) throws ExecutionException, InterruptedException {
    log.info("address = {}", address);
    final String topic = "lpushWrite";
    SinkTaskContext context = mock(SinkTaskContext.class);
    when(context.assignment()).thenReturn(ImmutableSet.of(new TopicPartition(topic, 1)));
    this.task.initialize(context);
    this.task.start(
            ImmutableMap.of(
                    RedisSinkConnectorConfig.HOSTS_CONFIG, String.format("%s:%s", address.getHostString(), address.getPort()),
                    RedisSinkConnectorConfig.INSERTION_TYPE_CONF, SinkOperation.Type.LPUSH.name()
            )
    );

    final int count = 50;
    final Map<String, List<String>> expected = new LinkedHashMap<>(count);
    final List<SinkRecord> records = new ArrayList<>(count);

    for (int i = 0; i < count; i++) {
      final String key = String.format("lpushWrite%s", i);
      final String value = String.format("This is value %s", i);
      final String otherValue = String.format("This is another value %s", i);
      records.add(
              write(topic,
                      new SchemaAndValue(Schema.STRING_SCHEMA, key),
                      new SchemaAndValue(Schema.STRING_SCHEMA, value)
              )
      );
      records.add(
              write(topic,
                      new SchemaAndValue(Schema.STRING_SCHEMA, key),
                      new SchemaAndValue(Schema.STRING_SCHEMA, otherValue)
              )
      );

      expected.put(key, Arrays.asList(value, otherValue ));
    }
    this.task.put(records);

    final byte[][] keys = expected.keySet().stream()
            .map(s -> s.getBytes(Charsets.UTF_8))
            .toArray(byte[][]::new);
    for(byte[] byteKey: keys) {
      RedisFuture<List<byte[]>> result = this.task.session.asyncCommands().lrange(byteKey, 0, -1);
      List<byte[]> actual = result.get();
      assertEquals(2, actual.size());
      final String key = new String(byteKey, Charsets.UTF_8);
      final String value = new String(actual.get(0), Charsets.UTF_8);
      final String otherValue = new String(actual.get(1), Charsets.UTF_8);
      final List<String> expectedValue = expected.get(key);
      assertEquals(value, expectedValue.get(1), String.format("Value for key(%s) does not match.", key));
      assertEquals(otherValue, expectedValue.get(0), String.format("Value for key(%s) does not match.", key));
    }
  }

  @Test
  public void rpushWrite(@Port(container = "redis", internalPort = 6379) InetSocketAddress address) throws ExecutionException, InterruptedException {
    log.info("address = {}", address);
    final String topic = "rpushWrite";
    SinkTaskContext context = mock(SinkTaskContext.class);
    when(context.assignment()).thenReturn(ImmutableSet.of(new TopicPartition(topic, 1)));
    this.task.initialize(context);
    this.task.start(
            ImmutableMap.of(
                    RedisSinkConnectorConfig.HOSTS_CONFIG, String.format("%s:%s", address.getHostString(), address.getPort()),
                    RedisSinkConnectorConfig.INSERTION_TYPE_CONF, SinkOperation.Type.RPUSH.name()
            )
    );

    final int count = 50;
    final Map<String, List<String>> expected = new LinkedHashMap<>(count);
    final List<SinkRecord> records = new ArrayList<>(count);

    for (int i = 0; i < count; i++) {
      final String key = String.format("rpushWrite%s", i);
      final String value = String.format("This is value %s", i);
      final String otherValue = String.format("This is another value %s", i);
      records.add(
              write(topic,
                      new SchemaAndValue(Schema.STRING_SCHEMA, key),
                      new SchemaAndValue(Schema.STRING_SCHEMA, value)
              )
      );
      records.add(
              write(topic,
                      new SchemaAndValue(Schema.STRING_SCHEMA, key),
                      new SchemaAndValue(Schema.STRING_SCHEMA, otherValue)
              )
      );

      expected.put(key, Arrays.asList(value, otherValue ));
    }
    this.task.put(records);

    final byte[][] keys = expected.keySet().stream()
            .map(s -> s.getBytes(Charsets.UTF_8))
            .toArray(byte[][]::new);
    for(byte[] byteKey: keys) {
      RedisFuture<List<byte[]>> result = this.task.session.asyncCommands().lrange(byteKey, 0, -1);
      List<byte[]> actual = result.get();
      assertEquals(2, actual.size());
      final String key = new String(byteKey, Charsets.UTF_8);
      final String value = new String(actual.get(0), Charsets.UTF_8);
      final String otherValue = new String(actual.get(1), Charsets.UTF_8);
      final List<String> expectedValue = expected.get(key);
      assertEquals(value, expectedValue.get(0), String.format("Value for key(%s) does not match.", key));
      assertEquals(otherValue, expectedValue.get(1), String.format("Value for key(%s) does not match.", key));
    }
  }

  @AfterEach
  public void after() {
    if (null != this.task) {
      this.task.stop();
    }
  }

}
