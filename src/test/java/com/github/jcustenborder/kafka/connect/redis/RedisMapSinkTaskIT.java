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
import com.palantir.docker.compose.connection.Cluster;
import io.lettuce.core.LettuceFutures;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public abstract class RedisMapSinkTaskIT extends AbstractSinkTaskIntegrationTest<RedisMapSinkTask> {
  static final Schema VALUE_SCHEMA = SchemaBuilder.struct()
      .field("ident", Schema.STRING_SCHEMA)
      .field("region", Schema.STRING_SCHEMA)
      .field("latitude", Schema.STRING_SCHEMA)
      .field("longitude", Schema.STRING_SCHEMA)
      .build();
  private static final Logger log = LoggerFactory.getLogger(RedisMapSinkTaskIT.class);

  @Override
  protected RedisMapSinkTask createTask() {
    return new RedisMapSinkTask();
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
  public void putWrite() throws ExecutionException, InterruptedException, TimeoutException, IOException {
    this.task.start(this.settings);

    final List<TestLocation> locations = TestLocation.loadLocations();
    final AtomicLong offset = new AtomicLong(1L);
    final List<SinkRecord> writes = locations.stream()
        .map(l -> structWrite(l, topic, 1, offset))
        .collect(Collectors.toList());
    this.task.put(writes);
    assertExists(locations);
  }

  void assertExists(List<TestLocation> locations) throws InterruptedException, ExecutionException, TimeoutException {
    byte[][] fieldNames = VALUE_SCHEMA.fields().stream()
        .map(f -> f.name().getBytes(Charsets.UTF_8))
        .toArray(byte[][]::new);
    final AtomicInteger foundLocations = new AtomicInteger(0);

    List<CompletableFuture<?>> futures = new ArrayList<>();

    for (TestLocation location : locations) {
      byte[] key = location.ident.getBytes(StandardCharsets.UTF_8);

      futures.add(
          this.task.session.hash().hgetall(key).thenAccept(byteMap -> {
            foundLocations.incrementAndGet();

          }).toCompletableFuture()
      );
    }

    this.task.session.flushCommands();
    LettuceFutures.awaitAll(30, TimeUnit.SECONDS, futures.toArray(new CompletableFuture<?>[futures.size()]));
    assertEquals(locations.size(), foundLocations.get());

  }

  void assertNotExists(List<TestLocation> locations) throws InterruptedException, ExecutionException, TimeoutException {
    final AtomicLong results = new AtomicLong(0);
    CompletableFuture<?>[] futures = locations.stream()
        .map(e -> e.ident.getBytes(StandardCharsets.UTF_8))
        .map(b-> this.task.session.key().exists(b).thenAccept(results::addAndGet).toCompletableFuture())
        .toArray(CompletableFuture<?>[]::new);
    log.info("Waiting for {} futures", futures.length);
    this.task.session.flushCommands();
    LettuceFutures.awaitAll(30, TimeUnit.SECONDS, futures);
    assertEquals(0, results.get());
  }

  @Test
  public void putDelete() throws
      ExecutionException, InterruptedException, IOException, TimeoutException {

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
    Map<TopicPartition, OffsetAndMetadata> offsets = TestUtils.offsets(writes);
    this.task.flush(offsets);

    assertExists(locations);

    this.task.put(deletes);
    offsets = TestUtils.offsets(writes);
    this.task.flush(offsets);

    assertNotExists(locations);
  }
}
