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
import com.github.jcustenborder.kafka.connect.redis.healthchecks.RedisClusterHealthCheck;
import com.github.jcustenborder.kafka.connect.redis.healthchecks.RedisSentinelHealthCheck;
import com.github.jcustenborder.kafka.connect.redis.healthchecks.RedisStandardHealthCheck;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.docker.compose.connection.Cluster;
import io.lettuce.core.KeyValue;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.github.jcustenborder.kafka.connect.redis.TestUtils.offsets;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public abstract class RedisStreamsSinkTaskIT extends AbstractSinkTaskIntegrationTest<RedisStreamsSinkTask> {
  static final Schema VALUE_SCHEMA = SchemaBuilder.struct()
      .field("ident", Schema.STRING_SCHEMA)
      .field("region", Schema.STRING_SCHEMA)
      .field("latitude", Schema.STRING_SCHEMA)
      .field("longitude", Schema.STRING_SCHEMA)
      .build();
  private static final Logger log = LoggerFactory.getLogger(RedisStreamsSinkTaskIT.class);

  @Override
  protected RedisStreamsSinkTask createTask() {
    return new RedisStreamsSinkTask();
  }

  @Test
  public void emptyAssignment() throws ExecutionException, InterruptedException {
    this.task.start(this.settings);
  }


  @Test
  public void putEmpty() throws ExecutionException, InterruptedException {
    this.task.start(this.settings);
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
  public void putWrite() throws Exception {
    this.task.start(this.settings);

    final List<TestLocation> locations = TestLocation.loadLocations();
    final AtomicLong offset = new AtomicLong(1L);
    final List<SinkRecord> writes = locations.stream()
        .map(l -> structWrite(l, topic, 1, offset))
        .collect(Collectors.toList());
    this.task.put(writes);
    Map<TopicPartition, OffsetAndMetadata> offsets = offsets(writes);
    this.task.flush(offsets);

    XReadArgs.StreamOffset<byte[]> startOffset =
        XReadArgs.StreamOffset.from(
            topic.getBytes(StandardCharsets.UTF_8),
            "0-0"
        );

    try (RedisSession session = this.task.sessionFactory.createSession(this.task.config)) {
      List<StreamMessage<byte[], byte[]>> messages = session.streams().xread(startOffset).get(30, TimeUnit.SECONDS);
      assertNotNull(messages);
      assertEquals(locations.size(), messages.size());

      Map<String, Map<String, String>> expected = locations
          .stream()
          .map(location-> ImmutableMap.of(
              "ident", location.ident,
              "region", location.region,
              "latitude", Double.toString(location.latitude),
              "longitude", Double.toString(location.longitude)
          ))
          .collect(
              Collectors.toMap(
                  e -> e.get("ident"),
                  e -> e
              )
          );
      Map<String, Map<String, String>> actual = messages
          .stream()
          .map(m -> m.getBody().entrySet().stream().collect(
                  Collectors.toMap(
                      e -> new String(e.getKey(), StandardCharsets.UTF_8),
                      e -> new String(e.getValue(), StandardCharsets.UTF_8)
                  )
              )
          ).collect(
              Collectors.toMap(
                  e -> e.get("ident"),
                  e -> e
              )
          );

      assertEquals(expected, actual);
    }


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
      Map<String, String> actual = this.task.session.hash().hmget(key, fieldNames)
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
    final long written = this.task.session.key().exists(keys).get();
    assertEquals(0, written);
  }

  //  @Test
  public void putDelete() throws ExecutionException, InterruptedException, IOException, TimeoutException {
    this.task.start(this.settings);

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
}
