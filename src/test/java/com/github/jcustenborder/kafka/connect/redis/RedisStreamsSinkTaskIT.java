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
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Compose(
    dockerComposePath = "src/test/resources/docker-compose.yml"
)
public class RedisStreamsSinkTaskIT {
  static final Schema VALUE_SCHEMA = SchemaBuilder.struct()
      .field("ident", Schema.STRING_SCHEMA)
      .field("region", Schema.STRING_SCHEMA)
      .field("latitude", Schema.STRING_SCHEMA)
      .field("longitude", Schema.STRING_SCHEMA)
      .build();
  private static final Logger log = LoggerFactory.getLogger(RedisStreamsSinkTaskIT.class);
  RedisStreamsSinkTask task;

  @BeforeEach
  public void before() {
    this.task = new RedisStreamsSinkTask();
  }

  @Test
  public void emptyAssignment(@Port(container = "redis", internalPort = 6379) InetSocketAddress address) throws ExecutionException, InterruptedException {
    log.info("address = {}", address);
    final String topic = "putWrite";
    SinkTaskContext context = mock(SinkTaskContext.class);
    when(context.assignment()).thenReturn(ImmutableSet.of());
    this.task.initialize(context);
    this.task.start(
        ImmutableMap.of(RedisCacheSinkConnectorConfig.HOSTS_CONFIG, String.format("%s:%s", address.getHostString(), address.getPort()))
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
        ImmutableMap.of(RedisCacheSinkConnectorConfig.HOSTS_CONFIG, String.format("%s:%s", address.getHostString(), address.getPort()))
    );

    this.task.put(ImmutableList.of());
  }

  public SinkRecord structWrite(
      TestLocation location,
      String topic,
      int partition,
      AtomicLong offset
  ) {

    Struct value = new Struct(VALUE_SCHEMA)
        .put("ident", location.ident)
        .put("region", location.region)
        .put("latitude", Double.toString(location.latitude))
        .put("longitude", Double.toString(location.longitude));

    return new SinkRecord(topic, partition,
        Schema.STRING_SCHEMA, location.ident,
        value.schema(), value,
        offset.incrementAndGet());
  }


  public SinkRecord structDelete(
      TestLocation location,
      String topic,
      int partition,
      AtomicLong offset
  ) {
    return new SinkRecord(topic, partition,
        Schema.STRING_SCHEMA, location.ident,
        null, null,
        offset.incrementAndGet());
  }

  @Test
  public void putWrite(@Port(container = "redis", internalPort = 6379) InetSocketAddress address) throws ExecutionException, InterruptedException, TimeoutException, IOException {
    log.info("address = {}", address);
    final String topic = "putWrite";
    SinkTaskContext context = mock(SinkTaskContext.class);
    when(context.assignment()).thenReturn(ImmutableSet.of(new TopicPartition(topic, 1)));
    this.task.initialize(context);
    this.task.start(
        ImmutableMap.of(RedisCacheSinkConnectorConfig.HOSTS_CONFIG, String.format("%s:%s", address.getHostString(), address.getPort()))
    );

    final List<TestLocation> locations = TestLocation.loadLocations();
    final AtomicLong offset = new AtomicLong(1L);
    final List<SinkRecord> writes = locations.stream()
        .map(l -> structWrite(l, topic, 1, offset))
        .collect(Collectors.toList());
    this.task.put(writes);

    XReadArgs.StreamOffset<byte[]> startOffset =
        XReadArgs.StreamOffset.from(
            topic.getBytes(Charsets.UTF_8),
            "0-0"
        );
    List<StreamMessage<String, String>> messages = this.task.session.asyncCommands().xread(startOffset).get(30, TimeUnit.SECONDS)
        .stream()
        .map(this::convert)
        .collect(Collectors.toList());
    assertNotNull(messages);
    assertEquals(locations.size(), messages.size());
    IntStream.range(0, locations.size() - 1).forEach(index -> {
      TestLocation location = locations.get(index);
      Map<String, String> expected = ImmutableMap.of(
          "ident", location.ident,
          "region", location.region,
          "latitude", Double.toString(location.latitude),
          "longitude", Double.toString(location.longitude)
      );
      StreamMessage<String, String> actual = messages.get(index);
      assertEquals(expected, actual.getBody());
    });
  }

  StreamMessage<String, String> convert(StreamMessage<byte[], byte[]> input) {
    Map<String, String> body = input.getBody()
        .entrySet()
        .stream()
        .collect(Collectors.toMap(e -> new String(e.getKey(), Charsets.UTF_8), e -> new String(e.getValue(), Charsets.UTF_8)));
    return new StreamMessage<>(new String(input.getStream(), Charsets.UTF_8), input.getId(), body);
  }

  void assertExists(List<TestLocation> locations) throws InterruptedException, ExecutionException, TimeoutException {
    byte[][] fieldNames = VALUE_SCHEMA.fields().stream()
        .map(f -> f.name().getBytes(Charsets.UTF_8))
        .toArray(byte[][]::new);
    for (TestLocation location : locations) {
      byte[] key = location.ident.getBytes(Charsets.UTF_8);
      Map<String, String> actual = this.task.session.asyncCommands().hmget(key, fieldNames)
          .get(30, TimeUnit.SECONDS)
          .stream()
          .map(TestUtils::toString)
          .collect(Collectors.toMap(KeyValue::getKey, KeyValue::getValue));
      Map<String, String> expected = ImmutableMap.of(
          "ident", location.ident,
          "region", location.region,
          "latitude", Double.toString(location.latitude),
          "longitude", Double.toString(location.longitude)
      );
      assertEquals(expected, actual);
    }
  }

  void assertNotExists(List<TestLocation> locations) throws InterruptedException, ExecutionException, TimeoutException {
    byte[][] keys = locations.stream()
        .map(e -> e.ident.getBytes(Charsets.UTF_8))
        .toArray(byte[][]::new);
    final long written = this.task.session.asyncCommands().exists(keys).get();
    assertEquals(0, written);
  }

  //  @Test
  public void putDelete(@Port(container = "redis", internalPort = 6379) InetSocketAddress address) throws
      ExecutionException, InterruptedException, IOException, TimeoutException {
    log.info("address = {}", address);
    final String topic = "putDelete";
    SinkTaskContext context = mock(SinkTaskContext.class);
    when(context.assignment()).thenReturn(ImmutableSet.of(new TopicPartition(topic, 1)));
    this.task.initialize(context);
    this.task.start(
        ImmutableMap.of(RedisCacheSinkConnectorConfig.HOSTS_CONFIG, String.format("%s:%s", address.getHostString(), address.getPort()))
    );

    final List<TestLocation> locations = TestLocation.loadLocations();
    final AtomicLong offset = new AtomicLong(1L);
    final List<SinkRecord> writes = locations.stream()
        .map(l -> structWrite(l, topic, 1, offset))
        .collect(Collectors.toList());
    final List<SinkRecord> deletes = locations.stream()
        .map(l -> structDelete(l, topic, 1, offset))
        .collect(Collectors.toList());
    this.task.put(writes);
    assertExists(locations);
    this.task.put(deletes);
    assertNotExists(locations);
  }

  @AfterEach
  public void after() {
    if (null != this.task) {
      this.task.stop();
    }
  }

}
